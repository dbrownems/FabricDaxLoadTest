"""Per-run plotting helpers (latency / QPS / users)."""

from __future__ import annotations

from pathlib import Path


def plot_run(csv_path: str | Path, *, title: str | None = None):
    """Plot bucketed latency band + QPS + active-user count from a LoadGen CSV.

    Mirrors the chart that used to live in notebook cell 6: a 3-panel
    figure where the top panel shows the per-bucket min/max latency band
    + mean line, the middle stacks success vs error QPS, and the bottom
    plots active users over time. Returns the matplotlib Figure so the
    caller can save / restyle it. Pandas + matplotlib imports are
    deferred so the wheel can be imported in environments without them.
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
    n_buckets = min(100, max(1, len(df)))
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

    fig, (ax1, ax2, ax3) = plt.subplots(
        3, 1, figsize=(14, 10), sharex=True,
        gridspec_kw={"height_ratios": [3, 1, 1]})
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
    ax3.set_xlabel("Time (seconds)")
    ax3.legend(loc="upper left")
    ax3.grid(True, alpha=0.3)
    ax3.set_ylim(bottom=0)

    fig.tight_layout()
    return fig
