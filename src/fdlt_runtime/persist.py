"""Delta-table writer for FabricDaxLoadTest runs.

Pulled from notebook cell 5b. Writes 5 tables:

  LoadTests        — 1 row per logical test (MERGE on LoadTestId)
  LoadTestRuns     — 1 row per run (MERGE on RunId)
  LoadTestQueries  — Load Test Scenario snapshots (insert-only,
                     keyed (LoadTestId, RunId, QueryHash))
  QueryExecutions  — per-attempt facts. Generic across sources;
                     keyed (Source, SourceId, ...). For LoadTest runs:
                     Source="LoadTestRun", SourceId=<RunId>.
                     DELETE WHERE (Source, SourceId) match / INSERT.
  TraceEvents      — engine-side XMLA trace events. Same (Source, SourceId)
                     scheme as QueryExecutions so a future Trace Capture
                     workflow appends here without schema churn.

Per-run snapshots: each notebook execution mints a fresh RunId and the
queries DataFrame is keyed by (LoadTestId, RunId, QueryHash). Re-runs
where the scenario changed retain the prior snapshot intact, so the
LoadTestRuns row's `ScenarioHash` always resolves to a fully captured
set of queries.

Schema is forward-compatible with §1.6 unified-trace: every row carries
OwnerType/OwnerId/OwnerKey for the eventual TraceOwners-driven slicer.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import pandas as pd

from .runner import RunResult


@dataclass
class WriteSummary:
    """Returned by `write_run` so the notebook can print the recap."""

    load_test_id: str
    run_id: str
    scenario_hash: str
    queries_written: int
    executions_written: int
    trace_events_written: int
    table_base: str


def _hash_query(text: str) -> str:
    return hashlib.sha256(text.strip().encode("utf-8")).hexdigest()


def _scenario_hash(query_hashes: Iterable[str]) -> str:
    return hashlib.sha256(
        ("\u0001".join(query_hashes)).encode("utf-8")
    ).hexdigest()


def write_run(
    spark,
    *,
    table_base: str,
    workspace_id: str,
    workspace_name: str,
    notebook_id: Optional[str],
    notebook_name: Optional[str],
    load_test_name: str,
    load_test_description: str,
    target_workspace: str,
    target_dataset: str,
    target_replica: str,
    xmla: str,
    queries,
    user_count: int,
    duration_sec: int,
    ramp_sec: int,
    concurrent_queries_per_user: int,
    pause_iter_ms: int,
    pause_query_ms: int,
    skip_results: bool,
    run: RunResult,
    runtime_version: str,
) -> WriteSummary:
    """Persist the 4 lakehouse tables for a single run."""
    from pyspark.sql import Row
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, LongType,
        DoubleType, TimestampType,
    )
    from delta.tables import DeltaTable

    # LoadTestId == the Fabric notebook item GUID (§1.6 Table 1). Fall back
    # to a deterministic UUID when the runtime can't surface a notebook id.
    if notebook_id:
        load_test_id = str(notebook_id)
    else:
        load_test_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL, f"fdlt://{workspace_id}/{load_test_name}"))
    notebook_label = notebook_name or load_test_name

    query_hashes = [_hash_query(q) for q in queries]
    scenario_hash = _scenario_hash(query_hashes)

    summary = (run.result_envelope or {}).get("summary", {}) if run.result_envelope else {}
    lat = summary.get("latency", {}) or {}
    run_status = (
        "Aborted" if run.error_envelope is not None
        else ("Cancelled" if run.returncode == 130 else "Completed")
    )
    abort_reason = (run.error_envelope or {}).get("message", "") if run.error_envelope else ""

    # Read the per-query CSV so we can compute the real run window.
    df = pd.read_csv(run.csv_path)
    if len(df) > 0:
        started_at = pd.to_datetime(df["StartUtc"].min(), utc=True).to_pydatetime()
        ended_at = pd.to_datetime(df["EndUtc"].max(), utc=True).to_pydatetime()
    else:
        started_at = run.started_at
        ended_at = run.ended_at

    if not run.run_id:
        raise RuntimeError(
            "RunResult.run_id is None — LoadGen never emitted a `started` "
            "envelope. The CSV cannot be joined to a stable run identity. "
            "Inspect run.stderr_tail for the underlying failure.")
    run_id = run.run_id

    def _path(name: str) -> str:
        return f"{table_base}/{name}"

    def _upsert(df_, name: str, merge_keys):
        path = _path(name)
        # All four tables are tiny per run (≤ ~10 MB raw for executions,
        # ≤ ~50 MB for traces). Coalesce to 1 partition before write so
        # each Delta commit appends one Parquet file rather than N tiny
        # ones (Spark's post-shuffle default is 200). Keeps file count
        # low for VACUUM/OPTIMIZE and Direct Lake transcoding.
        df_ = df_.coalesce(1)
        if DeltaTable.isDeltaTable(spark, path):
            # mergeSchema lets MERGE widen the target with any new
            # columns we added since the table was first created
            # (column renames in particular leave the old column behind
            # as null in newer rows, but at least the write succeeds —
            # versus a hard schema-mismatch failure).
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            tgt = DeltaTable.forPath(spark, path)
            on = " AND ".join(f"t.{k}=s.{k}" for k in merge_keys)
            (tgt.alias("t")
                .merge(df_.alias("s"), on)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
        else:
            df_.write.format("delta").mode("overwrite").save(path)

    def _replace_for_run(df_, name: str, run_id_: str):
        path = _path(name)
        # See _upsert: single-file writes for tiny per-run tables.
        df_ = df_.coalesce(1)
        if DeltaTable.isDeltaTable(spark, path):
            DeltaTable.forPath(spark, path).delete(f"RunId = '{run_id_}'")
            (df_.write.format("delta").mode("append")
                .option("mergeSchema", "true").save(path))
        else:
            (df_.write.format("delta").mode("overwrite")
                .option("mergeSchema", "true").save(path))

    def _replace_for_source(df_, name: str, source: str, source_id: str):
        """Generic delete-and-append for tables keyed by (Source, SourceId).

        Writes from any data origin (LoadTest run, Trace Capture, etc.)
        share the same physical table; the (Source, SourceId) pair is
        what makes a write idempotent in re-runs.
        """
        path = _path(name)
        df_ = df_.coalesce(1)
        if DeltaTable.isDeltaTable(spark, path):
            DeltaTable.forPath(spark, path).delete(
                f"Source = '{source}' AND SourceId = '{source_id}'")
            (df_.write.format("delta").mode("append")
                .option("mergeSchema", "true").save(path))
        else:
            (df_.write.format("delta").mode("overwrite")
                .option("mergeSchema", "true").save(path))

    # Build all 4 DataFrames first, then issue the writes concurrently
    # via a ThreadPoolExecutor. Spark sessions are thread-safe and the
    # JVM driver schedules jobs from different Python threads in
    # parallel; the writes are tiny and most of their wall-clock cost
    # is the Delta-log + commit overhead, not data shuffling. Doing
    # them in parallel cuts cell-3 write latency from ~4× a single
    # write to roughly 1× the slowest write.
    # Ref: https://milescole.dev/data-engineering/2024/04/26/Fabric-Concurrency-Showdown-RunMultiple-vs-ThreadPool.html

    # LoadTests --------------------------------------------------------------
    load_tests_df = spark.createDataFrame([Row(
        LoadTestId=load_test_id,
        Name=load_test_name,
        Description=load_test_description,
        WorkspaceId=workspace_id,
        WorkspaceName=workspace_name,
        NotebookId=load_test_id,
        NotebookName=notebook_label,
        TargetWorkspace=target_workspace,
        TargetDataset=target_dataset,
        SourceType="HandAuthored",
        QueryCount=len(queries),
        ScenarioHash=scenario_hash,
        LastRunAtUtc=started_at,
        LastRunId=run_id,
        Status="Active",
    )])

    # LoadTestRuns -----------------------------------------------------------
    runs_df = spark.createDataFrame([Row(
        RunId=run_id,
        LoadTestId=load_test_id,
        RunName=load_test_name,
        OwnerType="LoadTestRun",
        OwnerId=run_id,
        OwnerKey=f"LoadTestRun/{run_id}",
        ScenarioHash=scenario_hash,
        StartedAtUtc=started_at,
        EndedAtUtc=ended_at,
        TargetWorkspace=target_workspace,
        TargetDataset=target_dataset,
        XmlaEndpoint=xmla,
        Replica=target_replica or "",
        UserCount=int(user_count),
        DurationSec=int(duration_sec),
        RampSec=int(ramp_sec),
        ConcurrentQueriesPerUser=int(concurrent_queries_per_user),
        PauseIterMs=int(pause_iter_ms),
        PauseQueryMs=int(pause_query_ms),
        SkipResults=bool(skip_results),
        TotalQueries=int(summary.get("totalExecutions") or len(df)),
        SuccessfulQueries=int(summary.get("successfulExecutions") or
                              (int((df["Outcome"] == "Success").sum()) if len(df) else 0)),
        FailedQueries=int(summary.get("failedExecutions") or
                          (int((df["Outcome"] == "Error").sum()) if len(df) else 0)),
        Qps=float(summary.get("qps") or 0.0),
        Status=run_status,
        AbortReason=abort_reason,
        P50Ms=float(lat.get("median") or 0.0),
        P95Ms=float(lat.get("p95") or 0.0),
        P99Ms=float(lat.get("p99") or 0.0),
        MeanMs=float(lat.get("mean") or 0.0),
        RuntimeVersion=runtime_version,
    )])

    # LoadTestQueries — per-run snapshot (LoadTestId, RunId, QueryHash) ------
    queries_rows = [Row(
        LoadTestId=load_test_id,
        RunId=run_id,
        QueryIndex=i,
        QueryHash=query_hashes[i],
        QueryText=queries[i],
        SourceType="HandAuthored",
    ) for i in range(len(queries))]
    queries_df = spark.createDataFrame(queries_rows) if queries_rows else None

    # QueryExecutions --------------------------------------------------------
    if len(df) > 0:
        df2 = df.copy()
        df2["StartUtc"] = pd.to_datetime(df2["StartUtc"], utc=True)
        df2["EndUtc"] = pd.to_datetime(df2["EndUtc"], utc=True)
        df2["QueryHash"] = df2["QueryIndex"].apply(
            lambda i: query_hashes[int(i)] if 0 <= int(i) < len(query_hashes) else None)
        exec_schema = StructType([
            StructField("Source",               StringType(),    False),
            StructField("SourceId",             StringType(),    False),
            StructField("LoadTestId",           StringType(),    True),
            # UserIndex / QueryIndex / Iteration / QuerySeq are non-null for
            # LoadTestRun rows but NULL for TraceCapture rows (no
            # driver-assigned indices). Schema is nullable=true to support
            # both sources; load-test row builder still always populates them.
            StructField("UserIndex",            IntegerType(),   True),
            StructField("UserEmail",            StringType(),    True),
            StructField("QueryIndex",           IntegerType(),   True),
            StructField("QueryHash",            StringType(),    True),
            StructField("Iteration",            IntegerType(),   True),
            StructField("QuerySeq",             IntegerType(),   True),
            # Session identity. SessionId/RequestId are back-filled from
            # the QueryEnd trace row via (SourceId, QuerySeq) → ActivityId.
            # LogicalSessionId is a {LoadTestId}:{UserIndex} pseudo-id for
            # load tests (one logical session per virtual user) and is
            # computed by a window function over (UserEmail, StartUtc) at
            # end-of-capture for TraceCapture rows.
            StructField("SessionId",            StringType(),    True),
            StructField("RequestId",            StringType(),    True),
            StructField("LogicalSessionId",     StringType(),    True),
            StructField("StartUtc",             TimestampType(), False),
            StructField("EndUtc",               TimestampType(), True),
            StructField("StartTimeMs",          DoubleType(),    True),
            StructField("ClientDurationMs",     DoubleType(),    True),
            # QueryEnd-event totals (engine-wall + total CPU as the
            # server billed it; ≈ ExecutionMetrics.totalCpuTimeMs).
            StructField("EngineDurationMs",     LongType(),      True),
            StructField("EngineCpuMs",          LongType(),      True),
            # ExecutionMetrics breakdown — back-filled via RequestId map.
            StructField("SECpuMs",              LongType(),      True),
            StructField("FECpuMs",              LongType(),      True),
            StructField("ExecutionDelayMs",     LongType(),      True),
            StructField("PeakMemoryKB",         LongType(),      True),
            StructField("QueryResultRows",      LongType(),      True),
            # Raw ExecutionMetrics JSON passthrough so we don't lose any
            # fields AS emits in regimes we haven't tested (e.g. capacity
            # throttling, which we expect adds a throttle-delay field but
            # whose exact name we'll only know once we observe a throttled
            # run). Promote new fields to typed columns once their names
            # and semantics are confirmed. WALL-CLOCK BUDGET (ms):
            #   ClientDurationMs                                  total client wait
            #   = EngineDurationMs                                + network/serdes
            #     = EngineCpuMs                                   serialized CPU work (FE+SE)
            #       + (engine wall - engine CPU - exec delay      DQ wait + parallel SE wait + throttle
            #          - DirectQueryDurationMs)                   ^ residual is "unaccounted"
            #     + ExecutionDelayMs                              queueing for an engine worker slot
            #     + (throttle fields, TBD)                        capacity-router throttling, if any
            StructField("ExecutionMetricsJson", StringType(),    True),
            # VertiPaq SE aggregates per query. NOTE (v0.9.12+): the
            # LoadTest QueryRunner trace no longer subscribes to event 83
            # (VertiPaqSEQueryEnd) — it was the highest-volume event class
            # and the SE CPU it gave us is already covered by
            # ExecutionMetrics.vertipaqJobCpuTimeMs (-> SECpuMs). These
            # columns are kept in the schema so external TraceCapture
            # flows that DO capture event 83 can still populate them via
            # the same back-fill code below; for LoadTest runs they will
            # always be NULL.
            StructField("VertiPaqQueryCount",   IntegerType(),   True),
            StructField("VertiPaqDurationMs",   LongType(),      True),
            # DirectQuery aggregates per query (DirectLake/DQ models).
            StructField("DirectQueryCount",     IntegerType(),   True),
            StructField("DirectQueryDurationMs", LongType(),     True),
            StructField("DirectQueryCpuMs",     LongType(),      True),
            StructField("Outcome",              StringType(),    False),
            StructField("RowCount",             IntegerType(),   True),
            StructField("ResponseBytes",        LongType(),      True),
            StructField("ErrorMessage",         StringType(),    True),
            StructField("ActiveUsersAtStart",   IntegerType(),   True),
        ])
        rows = [Row(
            Source="LoadTestRun",
            SourceId=str(r["RunId"]),
            LoadTestId=load_test_id,
            UserIndex=int(r["UserIndex"]),
            UserEmail=str(r["UserEmail"]) if pd.notna(r["UserEmail"]) else None,
            QueryIndex=int(r["QueryIndex"]),
            QueryHash=r["QueryHash"],
            Iteration=int(r["Iteration"]),
            QuerySeq=int(r["QuerySeq"]) if "QuerySeq" in r and pd.notna(r["QuerySeq"]) else None,
            # SessionId / RequestId back-filled from QueryEnd trace below.
            # LogicalSessionId is deterministic for load tests: one logical
            # session per (LoadTest, virtual user).
            SessionId=None,
            RequestId=None,
            LogicalSessionId=(f"{load_test_id}:{int(r['UserIndex'])}"
                              if load_test_id is not None else None),
            StartUtc=r["StartUtc"].to_pydatetime(),
            EndUtc=r["EndUtc"].to_pydatetime() if pd.notna(r["EndUtc"]) else None,
            StartTimeMs=float(r["StartTimeMs"]) if pd.notna(r["StartTimeMs"]) else None,
            ClientDurationMs=float(r["DurationMs"]) if pd.notna(r["DurationMs"]) else None,
            # All trace-derived columns are back-filled below.
            EngineDurationMs=None, EngineCpuMs=None,
            SECpuMs=None, FECpuMs=None, ExecutionDelayMs=None,
            PeakMemoryKB=None, QueryResultRows=None,
            ExecutionMetricsJson=None,
            VertiPaqQueryCount=None, VertiPaqDurationMs=None,
            DirectQueryCount=None, DirectQueryDurationMs=None,
            DirectQueryCpuMs=None,
            Outcome=str(r["Outcome"]),
            RowCount=int(r["RowCount"]) if pd.notna(r["RowCount"]) else None,
            ResponseBytes=int(r["ResponseBytes"]) if pd.notna(r["ResponseBytes"]) else None,
            ErrorMessage=(str(r["ErrorMessage"])
                          if pd.notna(r["ErrorMessage"]) and str(r["ErrorMessage"])
                          else None),
            ActiveUsersAtStart=int(r["ActiveUsersAtStart"])
                              if pd.notna(r["ActiveUsersAtStart"]) else None,
        ) for _, r in df2.iterrows()]
        exec_df = spark.createDataFrame(rows, schema=exec_schema)
        executions_written = len(rows)
    else:
        exec_df = None
        executions_written = 0

    # TraceEvents — engine-side XMLA trace events captured by
    # TraceSubscriber (best-effort). Empty when --no-trace was set or
    # the trace failed to start. Schema mirrors the C# CSV emitter.
    trace_df = None
    trace_events_written = 0
    import os as _os
    if run.trace_csv_path and _os.path.exists(run.trace_csv_path):
        try:
            tdf = pd.read_csv(run.trace_csv_path)
        except pd.errors.EmptyDataError:
            tdf = pd.DataFrame()
        if len(tdf) > 0:
            tdf["UtcTimestamp"] = pd.to_datetime(tdf["UtcTimestamp"], utc=True)
            trace_schema = StructType([
                StructField("Source",           StringType(),    False),
                StructField("SourceId",         StringType(),    False),
                StructField("LoadTestId",       StringType(),    True),
                StructField("UtcTimestamp",     TimestampType(), False),
                StructField("EventClass",       StringType(),    True),
                StructField("DurationMs",       LongType(),      True),
                StructField("CpuMs",            LongType(),      True),
                StructField("ApplicationName",  StringType(),    True),
                StructField("UserName",         StringType(),    True),
                StructField("SessionId",        StringType(),    True),
                StructField("RequestId",        StringType(),    True),
                StructField("ActivityId",       StringType(),    True),
                StructField("DatabaseName",     StringType(),    True),
                StructField("TextData",         StringType(),    True),
            ])
            trace_rows = [Row(
                Source="LoadTestRun",
                SourceId=run_id,
                LoadTestId=load_test_id,
                UtcTimestamp=r["UtcTimestamp"].to_pydatetime(),
                EventClass=str(r["EventClass"]) if pd.notna(r["EventClass"]) else None,
                DurationMs=int(r["DurationMs"]) if pd.notna(r["DurationMs"]) else None,
                CpuMs=int(r["CpuMs"]) if pd.notna(r["CpuMs"]) else None,
                ApplicationName=str(r["ApplicationName"]) if pd.notna(r["ApplicationName"]) else None,
                UserName=str(r["UserName"]) if pd.notna(r["UserName"]) else None,
                SessionId=str(r["SessionId"]) if pd.notna(r["SessionId"]) else None,
                RequestId=str(r["RequestId"]) if pd.notna(r["RequestId"]) else None,
                ActivityId=(str(r["ActivityId"])
                            if "ActivityId" in r and pd.notna(r["ActivityId"])
                            else None),
                DatabaseName=str(r["DatabaseName"]) if pd.notna(r["DatabaseName"]) else None,
                TextData=str(r["TextData"]) if pd.notna(r["TextData"]) else None,
            ) for _, r in tdf.iterrows()]
            trace_df = spark.createDataFrame(trace_rows, schema=trace_schema)
            trace_events_written = len(trace_rows)

    # Engine-side back-fill: JOIN executions to several trace events on
    # (SourceId, RequestId). The QueryEnd trace row carries both ActivityId
    # (which decodes back to QuerySeq) and RequestId (the per-XMLA-request
    # id every engine event is also tagged with). We build a
    # (SourceId, QuerySeq) → RequestId mapping from QueryEnd, then aggregate
    # each downstream event class by RequestId and JOIN onto exec_df.
    # All JOINs are LEFT so a missing event class (e.g. no DQ on import
    # models) just leaves its columns NULL.
    if exec_df is not None and trace_df is not None:
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            StructType as _ST, StructField as _SF, LongType as _LT,
            StringType as _StrT,
        )

        # 1. QueryEnd → (SourceId, QuerySeq, RequestId, SessionId,
        #    EngineCpuMs, EngineDurationMs)
        #    QuerySeq is the last 4 hex bytes of ActivityId, decoded big-endian.
        trace_qe = (trace_df
            .where((F.col("EventClass") == F.lit("QueryEnd")) &
                   F.col("ActivityId").isNotNull())
            .withColumn(
                "QuerySeq",
                F.conv(
                    F.substring(
                        F.regexp_replace(F.col("ActivityId"), "-", ""),
                        25, 8),
                    16, 10).cast("int"))
            .select(
                F.col("SourceId"),
                F.col("QuerySeq"),
                F.col("RequestId"),
                F.col("SessionId"),
                F.col("CpuMs").alias("EngineCpuMs"),
                F.col("DurationMs").alias("EngineDurationMs"))
            .dropDuplicates(["SourceId", "QuerySeq"]))

        # The (SourceId, RequestId) → QuerySeq map for joining the rest.
        qe_map = trace_qe.select("SourceId", "RequestId", "QuerySeq")

        # 2. ExecutionMetrics — JSON in TextData. Parse once, project fields.
        em_schema = _ST([
            _SF("vertipaqJobCpuTimeMs",            _LT(), True),
            _SF("queryProcessingCpuTimeMs",        _LT(), True),
            _SF("executionDelayMs",                _LT(), True),
            _SF("approximatePeakMemConsumptionKB", _LT(), True),
            _SF("queryResultRows",                 _LT(), True),
        ])
        trace_em = (trace_df
            .where((F.col("EventClass") == F.lit("ExecutionMetrics")) &
                   F.col("RequestId").isNotNull() &
                   F.col("TextData").isNotNull())
            .withColumn("em", F.from_json(F.col("TextData"), em_schema))
            .select(
                F.col("SourceId"),
                F.col("RequestId"),
                F.col("em.vertipaqJobCpuTimeMs").alias("SECpuMs"),
                F.col("em.queryProcessingCpuTimeMs").alias("FECpuMs"),
                F.col("em.executionDelayMs").alias("ExecutionDelayMs"),
                F.col("em.approximatePeakMemConsumptionKB").alias("PeakMemoryKB"),
                F.col("em.queryResultRows").alias("QueryResultRows"),
                # Raw JSON passthrough — keeps fields we haven't parsed (e.g.
                # future throttling-related fields). One JSON blob per query.
                F.col("TextData").alias("ExecutionMetricsJson"))
            .dropDuplicates(["SourceId", "RequestId"])
            .join(qe_map, on=["SourceId", "RequestId"], how="inner")
            .drop("RequestId"))

        # 3. VertiPaqSEQueryEnd aggregates — count + sum(duration) per query.
        trace_vpq = (trace_df
            .where((F.col("EventClass") == F.lit("VertiPaqSEQueryEnd")) &
                   F.col("RequestId").isNotNull())
            .groupBy("SourceId", "RequestId")
            .agg(
                F.count(F.lit(1)).cast("int").alias("VertiPaqQueryCount"),
                F.sum(F.col("DurationMs")).cast("long").alias("VertiPaqDurationMs"))
            .join(qe_map, on=["SourceId", "RequestId"], how="inner")
            .drop("RequestId"))

        # 4. (VertiPaqSEQueryCacheMatch dropped from XMLA in v0.9.4 to
        #    reduce trace volume — engine silently stops emitting events
        #    once its rowset buffer fills under high event-rate load.
        #    The VertiPaqCacheHits column was removed from QueryExecutions
        #    schema in the same release.)

        # 5. DirectQueryEnd aggregates (Direct Lake / DQ models only).
        trace_dq = (trace_df
            .where((F.col("EventClass") == F.lit("DirectQueryEnd")) &
                   F.col("RequestId").isNotNull())
            .groupBy("SourceId", "RequestId")
            .agg(
                F.count(F.lit(1)).cast("int").alias("DirectQueryCount"),
                F.sum(F.col("DurationMs")).cast("long").alias("DirectQueryDurationMs"),
                F.sum(F.col("CpuMs")).cast("long").alias("DirectQueryCpuMs"))
            .join(qe_map, on=["SourceId", "RequestId"], how="inner")
            .drop("RequestId"))

        # Drop the placeholder NULL columns and LEFT JOIN each aggregate
        # back onto exec_df by (SourceId, QuerySeq).
        # SessionId/RequestId are placeholders too (Row builder set them to
        # None) — back-filled from QueryEnd along with EngineCpuMs etc.
        backfill_drop = [
            "SessionId", "RequestId",
            "EngineCpuMs", "EngineDurationMs",
            "SECpuMs", "FECpuMs", "ExecutionDelayMs",
            "PeakMemoryKB", "QueryResultRows", "ExecutionMetricsJson",
            "VertiPaqQueryCount", "VertiPaqDurationMs",
            "DirectQueryCount", "DirectQueryDurationMs", "DirectQueryCpuMs",
        ]
        exec_df = exec_df.drop(*backfill_drop)
        # trace_qe already carries SessionId + RequestId — join the whole
        # thing (don't drop RequestId like we did before).
        for side in (
            trace_qe,
            trace_em, trace_vpq, trace_dq,
        ):
            exec_df = exec_df.join(side, on=["SourceId", "QuerySeq"], how="left")

        # Reassert column order so the final DataFrame matches exec_schema.
        exec_df = exec_df.select(*[f.name for f in exec_schema.fields])

    # Fan out the writes. Each task is independent (different Delta path,
    # no foreign-key dependency between tables) so a ThreadPool gets the
    # benefit without any locking. We use a fair scheduling pool so a
    # long MERGE doesn't starve the other writes.
    from concurrent.futures import ThreadPoolExecutor, as_completed
    write_tasks = [
        ("LoadTests",                lambda: _upsert(load_tests_df, "LoadTests", ["LoadTestId"])),
        ("LoadTestRuns",             lambda: _upsert(runs_df, "LoadTestRuns", ["RunId"])),
    ]
    if queries_df is not None:
        write_tasks.append(
            ("LoadTestQueries",
             lambda: _replace_for_run(queries_df, "LoadTestQueries", run_id)))
    if exec_df is not None:
        write_tasks.append(
            ("QueryExecutions",
             lambda: _replace_for_source(
                 exec_df, "QueryExecutions", "LoadTestRun", run_id)))
    if trace_df is not None:
        write_tasks.append(
            ("TraceEvents",
             lambda: _replace_for_source(
                 trace_df, "TraceEvents", "LoadTestRun", run_id)))

    # Hint the Spark scheduler to interleave jobs from concurrent threads
    # rather than queue them strictly FIFO. Safe to set per-call; reverts
    # at session teardown.
    try:
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
    except Exception:
        pass

    errors = []
    with ThreadPoolExecutor(max_workers=len(write_tasks),
                            thread_name_prefix="fdlt-delta") as ex:
        futures = {ex.submit(fn): name for name, fn in write_tasks}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                fut.result()
            except Exception as e:
                errors.append((name, e))
    if errors:
        msg = "; ".join(f"{n}: {type(e).__name__}: {e}" for n, e in errors)
        raise RuntimeError(f"Delta write failed for: {msg}")

    return WriteSummary(
        load_test_id=load_test_id,
        run_id=run_id,
        scenario_hash=scenario_hash,
        queries_written=len(queries_rows),
        executions_written=executions_written,
        trace_events_written=trace_events_written,
        table_base=table_base,
    )
