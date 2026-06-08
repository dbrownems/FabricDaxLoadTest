"""Per-run plotting helpers (latency / QPS / users / CPU)."""

from __future__ import annotations

import math
from pathlib import Path


_NICE_BUCKETS_S = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600]


def _pick_bucket_size_s(duration_s: float, target_buckets: int = 20) -> float:
    """Pick a 'nice' bucket size so plots show ~20 buckets across the run.

    Snaps up to the smallest value in {1, 2, 5, 10, 15, 30, 60, 120, 300, 600}
    that yields <= target_buckets buckets. Examples: 60s run -> 5s buckets
    (12 buckets), 295s run -> 15s buckets (~20 buckets), 1200s run -> 60s
    buckets (20 buckets). For a fixed bucket count cap call sites used to
    use 100 buckets which was too granular for runs longer than ~2 min.
    """
    raw = max(1.0, duration_s / max(1, target_buckets))
    for n in _NICE_BUCKETS_S:
        if n >= raw:
            return float(n)
    return float(_NICE_BUCKETS_S[-1])


def plot_run(csv_path: str | Path, *, title: str | None = None,
             trace_csv_path: str | Path | None = None):
    """Plot bucketed latency band + QPS + active-user count from a LoadGen CSV.

    Mirrors the chart that used to live in notebook cell 6: a 3-panel
    figure where the top panel shows the per-bucket min/max latency band
    + mean line, the middle stacks success vs error QPS, and the bottom
    plots active users over time. When ``trace_csv_path`` is supplied
    and contains ``QueryEnd`` events with CPU data, an additional
    bottom panel plots **CPU-seconds per second** (i.e. effective
    parallel CPU consumption) — the most useful single metric for
    capacity-utilization assessment, since Fabric Capacity CU is just a
    region/SKU-specific multiplier on engine CPU. Returns the
    matplotlib Figure so the caller can save / restyle it. Pandas +
    matplotlib imports are deferred so the wheel can be imported in
    environments without them.
    """
    import pandas as pd
    import matplotlib.pyplot as plt

    df = pd.read_csv(csv_path)
    if df.empty:
        raise RuntimeError(f"Telemetry CSV is empty: {csv_path}")

    print(
        f"Records: {len(df):,}  "
        f"Success: {(df.Outcome=='Success').sum():,}  "
        f"Error: {(df.Outcome=='Error').sum():,}"
    )

    t_min, t_max = df.StartTimeMs.min(), df.StartTimeMs.max()
    duration_s = max((t_max - t_min) / 1000, 1)
    bucket_size_s = _pick_bucket_size_s(duration_s)
    n_buckets = max(1, int(math.ceil(duration_s / bucket_size_s)))
    n_buckets = min(n_buckets, max(1, len(df)))
    df["bucket"] = pd.cut(df.StartTimeMs, bins=n_buckets, labels=False)

    ok = df[df.Outcome == "Success"]
    err = df[df.Outcome == "Error"]
    agg = ok.groupby("bucket").agg(
        count=("DurationMs", "count"), mean_ms=("DurationMs", "mean"),
        min_ms=("DurationMs", "min"), max_ms=("DurationMs", "max"),
        t=("StartTimeMs", "mean"),
    ).reset_index()
    errs = err.groupby("bucket").agg(err_count=("DurationMs", "count")).reset_index()
    users = df.groupby("bucket").agg(
        active_users=("ActiveUsersAtStart", "max")).reset_index()
    agg = (agg.merge(errs, on="bucket", how="left")
              .merge(users, on="bucket", how="left").fillna(0))
    agg["time_s"] = (agg.t - t_min) / 1000

    # Optional CPU-seconds-per-second series from the engine trace.
    cpu_x, cpu_y, cpu_bw = _cpu_per_second(trace_csv_path, df, duration_s)
    have_cpu = cpu_x is not None and len(cpu_x) > 0

    n_panels = 4 if have_cpu else 3
    height_ratios = [3, 1, 1, 2] if have_cpu else [3, 1, 1]
    fig, axes = plt.subplots(
        n_panels, 1, figsize=(14, 10 if not have_cpu else 13), sharex=True,
        gridspec_kw={"height_ratios": height_ratios})
    ax1, ax2, ax3 = axes[0], axes[1], axes[2]
    ax1.fill_between(agg.time_s, agg.min_ms, agg.max_ms,
                     alpha=0.15, color="steelblue", label="min–max")
    ax1.plot(agg.time_s, agg.mean_ms, color="steelblue", linewidth=1.5, label="mean")
    ax1.plot(agg.time_s, agg.max_ms, color="coral",
             linewidth=0.8, alpha=0.7, label="max")
    ax1.set_ylabel("Latency (ms)")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)
    if title:
        ax1.set_title(title)

    bw = duration_s / n_buckets if n_buckets > 0 else 1
    ax2.bar(agg.time_s, agg["count"] / bw, width=bw * 0.9,
            color="steelblue", alpha=0.6, label="QPS (success)")
    if agg.err_count.sum() > 0:
        ax2.bar(agg.time_s, agg.err_count / bw, width=bw * 0.9,
                bottom=agg["count"] / bw, color="red", alpha=0.6,
                label="QPS (error)")
    ax2.set_ylabel("Queries/sec")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.3)

    ax3.plot(agg.time_s, agg.active_users,
             color="green", linewidth=1.5, label="Active users")
    ax3.fill_between(agg.time_s, 0, agg.active_users, alpha=0.1, color="green")
    ax3.set_ylabel("Users")
    ax3.legend(loc="upper left")
    ax3.grid(True, alpha=0.3)
    ax3.set_ylim(bottom=0)
    if not have_cpu:
        ax3.set_xlabel("Time (seconds)")

    if have_cpu:
        ax4 = axes[3]
        ax4.bar(cpu_x, cpu_y, width=cpu_bw * 0.9,
                color="purple", alpha=0.65,
                label="Engine CPU (CPU-seconds / second)")
        ax4.set_ylabel("CPU s/s")
        ax4.set_xlabel("Time (seconds)")
        ax4.grid(True, alpha=0.3)
        ax4.set_ylim(bottom=0)
        peak = max(cpu_y) if len(cpu_y) else 0
        avg = (sum(cpu_y) * cpu_bw) / duration_s if duration_s > 0 else 0
        ax4.legend(loc="upper left",
                   title=f"peak={peak:.1f}  avg={avg:.1f}  "
                         f"bucket={cpu_bw:g}s")

    fig.tight_layout()
    # Detach the figure from pyplot so the inline backend's post-cell
    # "auto-flush all open figures" hook doesn't display it. Returning
    # the Figure then renders it exactly once via Jupyter's
    # rich-repr display path. Without close(), the figure renders twice:
    # once by the inline backend's Gcf flush, once by the cell-result repr.
    import matplotlib.pyplot as _plt
    _plt.close(fig)
    return fig


def _cpu_per_second(trace_csv_path, df, duration_s):
    """Bucket engine-side CPU into seconds-of-CPU-per-second-of-wallclock.

    Reads the trace CSV (best-effort; returns empty if missing/empty),
    filters to ``QueryEnd`` events with positive ``CpuMs`` and
    ``DurationMs``, computes each event's wallclock interval relative to
    test start (T0 derived from the executions CSV's
    ``StartUtc`` − ``StartTimeMs`` alignment), then **distributes the
    event's CpuMs uniformly over its [start, end] interval** so events
    that span multiple buckets contribute proportionally. Bucket size
    targets ~20 buckets across the run (snapped to a nice value like
    1, 5, 15, 30, 60s) — see ``_pick_bucket_size_s``.

    Returns ``(centers_s, cpu_per_sec, bucket_width_s)`` or
    ``(None, None, None)`` when no CPU data is available.
    """
    if not trace_csv_path:
        return None, None, None
    import os
    if not os.path.exists(trace_csv_path):
        return None, None, None

    import pandas as pd
    try:
        tdf = pd.read_csv(trace_csv_path)
    except pd.errors.EmptyDataError:
        return None, None, None
    if tdf.empty:
        return None, None, None
    tdf = tdf[(tdf.get("EventClass") == "QueryEnd") &
              (tdf.get("CpuMs", 0) > 0) &
              (tdf.get("DurationMs", 0) > 0)].copy()
    if tdf.empty:
        return None, None, None

    # Align trace UtcTimestamps with the executions test-start.
    # T0 (UTC) = StartUtc[i] - StartTimeMs[i]/1000  (any i; pick i=0 row's
    # earliest start to anchor).
    exe = df.copy()
    exe["StartUtc"] = pd.to_datetime(exe["StartUtc"], utc=True)
    if exe.empty:
        return None, None, None
    earliest = exe["StartTimeMs"].idxmin()
    t0 = (exe["StartUtc"].iloc[earliest]
          - pd.to_timedelta(exe["StartTimeMs"].iloc[earliest], unit="ms"))

    tdf["UtcTimestamp"] = pd.to_datetime(tdf["UtcTimestamp"], utc=True)
    tdf["end_s"] = (tdf["UtcTimestamp"] - t0).dt.total_seconds()
    tdf["start_s"] = tdf["end_s"] - tdf["DurationMs"] / 1000.0

    # Bucket sizing: shared with QPS/latency panels — see _pick_bucket_size_s.
    # Targets ~20 buckets, snapped to {1, 2, 5, 10, 15, 30, 60, 120, 300, 600}s.
    bucket_size_s = _pick_bucket_size_s(duration_s)
    n_buckets = max(1, int(math.ceil(duration_s / bucket_size_s)))
    cpu_ms = [0.0] * n_buckets

    for _, ev in tdf.iterrows():
        s, e, c = float(ev["start_s"]), float(ev["end_s"]), float(ev["CpuMs"])
        if e <= 0 or s >= duration_s or e <= s:
            # Event entirely before T0 (clock skew) or zero-length.
            continue
        s = max(s, 0.0)
        e = min(e, duration_s)
        span = e - s
        if span <= 0:
            continue
        cpu_per_s = c / span  # ms-of-CPU per second-of-wallclock for this event
        b0 = int(s // bucket_size_s)
        b1 = int(min(n_buckets - 1, e // bucket_size_s))
        for b in range(b0, b1 + 1):
            bucket_lo = b * bucket_size_s
            bucket_hi = bucket_lo + bucket_size_s
            overlap = max(0.0, min(e, bucket_hi) - max(s, bucket_lo))
            cpu_ms[b] += cpu_per_s * overlap

    centers = [(b + 0.5) * bucket_size_s for b in range(n_buckets)]
    cpu_per_sec = [(ms / 1000.0) / bucket_size_s for ms in cpu_ms]
    return centers, cpu_per_sec, bucket_size_s
