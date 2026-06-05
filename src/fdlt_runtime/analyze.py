"""Per-run plotting helpers (latency / QPS / users)."""

from __future__ import annotations

from pathlib import Path


def plot_run(csv_path: str | Path, *, title: str | None = None):
    """Plot 1-second latency + QPS + users from a LoadGen telemetry CSV.

    Returns the matplotlib Figure so the caller can save / restyle it.
    Imports of pandas + matplotlib are deferred so the wheel can be
    imported in environments that don't have either.
    """
    import pandas as pd
    import matplotlib.pyplot as plt

    df = pd.read_csv(csv_path)
    if df.empty:
        raise RuntimeError(f"Telemetry CSV is empty: {csv_path}")

    df["EndUtc"] = pd.to_datetime(df["EndUtc"], utc=True)
    df["StartUtc"] = pd.to_datetime(df["StartUtc"], utc=True)
    df["LatencyMs"] = (df["EndUtc"] - df["StartUtc"]).dt.total_seconds() * 1000.0

    bucket = df.set_index("EndUtc").groupby(pd.Grouper(freq="1s"))
    latency = bucket["LatencyMs"].agg(["count", "mean", "median"])
    latency["qps"] = latency["count"]
    users = bucket["UserIndex"].nunique() if "UserIndex" in df else None

    fig, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
    axes[0].plot(latency.index, latency["mean"], label="mean")
    axes[0].plot(latency.index, latency["median"], label="median")
    axes[0].set_ylabel("Latency (ms)")
    axes[0].legend()
    axes[1].plot(latency.index, latency["qps"], color="tab:green")
    axes[1].set_ylabel("QPS")
    if users is not None:
        axes[2].plot(users.index, users.values, color="tab:orange")
    axes[2].set_ylabel("Active users")
    axes[2].set_xlabel("Time (UTC)")
    if title:
        fig.suptitle(title)
    fig.tight_layout()
    return fig
