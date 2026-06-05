"""Delta-table writer for FabricDaxLoadTest runs.

Pulled from notebook cell 5b. Writes 4 tables:

  LoadTests                — 1 row per logical test (MERGE on LoadTestId)
  LoadTestRuns             — 1 row per run (MERGE on RunId)
  LoadTestQueries          — Load Test Scenario snapshots (insert-only,
                             keyed (LoadTestId, RunId, QueryHash))
  LoadTestQueryExecutions  — per-attempt facts (DELETE WHERE RunId / INSERT)

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
    queries_per_batch: int,
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
        if DeltaTable.isDeltaTable(spark, path):
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
        if DeltaTable.isDeltaTable(spark, path):
            DeltaTable.forPath(spark, path).delete(f"RunId = '{run_id_}'")
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
        WorkspaceName=target_workspace,
        DatasetName=target_dataset,
        XmlaEndpoint=xmla,
        Replica=target_replica or "",
        UserCount=int(user_count),
        DurationSec=int(duration_sec),
        RampSec=int(ramp_sec),
        QueriesPerBatch=int(queries_per_batch),
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

    # LoadTestQueryExecutions ------------------------------------------------
    if len(df) > 0:
        df2 = df.copy()
        df2["StartUtc"] = pd.to_datetime(df2["StartUtc"], utc=True)
        df2["EndUtc"] = pd.to_datetime(df2["EndUtc"], utc=True)
        df2["QueryHash"] = df2["QueryIndex"].apply(
            lambda i: query_hashes[int(i)] if 0 <= int(i) < len(query_hashes) else None)
        exec_schema = StructType([
            StructField("RunId",              StringType(),    False),
            StructField("LoadTestId",         StringType(),    False),
            StructField("UserIndex",          IntegerType(),   False),
            StructField("UserEmail",          StringType(),    True),
            StructField("QueryIndex",         IntegerType(),   False),
            StructField("QueryHash",          StringType(),    True),
            StructField("Iteration",          IntegerType(),   False),
            StructField("StartUtc",           TimestampType(), False),
            StructField("EndUtc",             TimestampType(), True),
            StructField("StartTimeMs",        DoubleType(),    True),
            StructField("ClientDurationMs",   DoubleType(),    True),
            StructField("Outcome",            StringType(),    False),
            StructField("RowCount",           IntegerType(),   True),
            StructField("ResponseBytes",      LongType(),      True),
            StructField("ErrorMessage",       StringType(),    True),
            StructField("ActiveUsersAtStart", IntegerType(),   True),
        ])
        rows = [Row(
            RunId=str(r["RunId"]),
            LoadTestId=load_test_id,
            UserIndex=int(r["UserIndex"]),
            UserEmail=str(r["UserEmail"]) if pd.notna(r["UserEmail"]) else None,
            QueryIndex=int(r["QueryIndex"]),
            QueryHash=r["QueryHash"],
            Iteration=int(r["Iteration"]),
            StartUtc=r["StartUtc"].to_pydatetime(),
            EndUtc=r["EndUtc"].to_pydatetime() if pd.notna(r["EndUtc"]) else None,
            StartTimeMs=float(r["StartTimeMs"]) if pd.notna(r["StartTimeMs"]) else None,
            ClientDurationMs=float(r["DurationMs"]) if pd.notna(r["DurationMs"]) else None,
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

    # LoadTestTraceEvents — engine-side XMLA trace events captured by
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
                StructField("RunId",            StringType(),    False),
                StructField("LoadTestId",       StringType(),    False),
                StructField("UtcTimestamp",     TimestampType(), False),
                StructField("EventClass",       StringType(),    True),
                StructField("DurationMs",       LongType(),      True),
                StructField("CpuMs",            LongType(),      True),
                StructField("ApplicationName",  StringType(),    True),
                StructField("UserName",         StringType(),    True),
                StructField("SessionId",        StringType(),    True),
                StructField("RequestId",        StringType(),    True),
                StructField("DatabaseName",     StringType(),    True),
                StructField("TextData",         StringType(),    True),
            ])
            trace_rows = [Row(
                RunId=run_id,
                LoadTestId=load_test_id,
                UtcTimestamp=r["UtcTimestamp"].to_pydatetime(),
                EventClass=str(r["EventClass"]) if pd.notna(r["EventClass"]) else None,
                DurationMs=int(r["DurationMs"]) if pd.notna(r["DurationMs"]) else None,
                CpuMs=int(r["CpuMs"]) if pd.notna(r["CpuMs"]) else None,
                ApplicationName=str(r["ApplicationName"]) if pd.notna(r["ApplicationName"]) else None,
                UserName=str(r["UserName"]) if pd.notna(r["UserName"]) else None,
                SessionId=str(r["SessionId"]) if pd.notna(r["SessionId"]) else None,
                RequestId=str(r["RequestId"]) if pd.notna(r["RequestId"]) else None,
                DatabaseName=str(r["DatabaseName"]) if pd.notna(r["DatabaseName"]) else None,
                TextData=str(r["TextData"]) if pd.notna(r["TextData"]) else None,
            ) for _, r in tdf.iterrows()]
            trace_df = spark.createDataFrame(trace_rows, schema=trace_schema)
            trace_events_written = len(trace_rows)

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
            ("LoadTestQueryExecutions",
             lambda: _replace_for_run(exec_df, "LoadTestQueryExecutions", run_id)))
    if trace_df is not None:
        write_tasks.append(
            ("LoadTestTraceEvents",
             lambda: _replace_for_run(trace_df, "LoadTestTraceEvents", run_id)))

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
