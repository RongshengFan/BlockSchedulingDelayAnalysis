#!/usr/bin/env python3
"""Analyze per-workload block CSV files and generate metrics/charts.

Input layout:
    output/data/*.csv

Output layout:
    output/chart/{overview,launch_offset,sched,load}/*.png
    output/chart/metrics/*.csv

Metrics are kept consistent with the existing analysis pipeline.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

BASE_DIR = Path(__file__).resolve().parent
WORKLOAD_ORDER = ["compute", "memory", "mixed", "sparse", "vgg16"]
WORKLOAD_PALETTE = {
    "compute": "#2a7afb",
    "memory": "#2ab0bc",
    "mixed": "#ffc401",
    "sparse": "#014415",
    "vgg16": "#f91625",
}
WORKLOAD_MARKERS = {
    "compute": "o",
    "memory": "s",
    "mixed": "D",
    "sparse": "^",
    "vgg16": "X",
}
WORKLOAD_LINESTYLES = {
    "compute": "-",
    "memory": "--",
    "mixed": "-.",
    "sparse": ":",
    "vgg16": (0, (6, 2)),
}


def ordered_workloads(values: list[str]) -> list[str]:
    known = [w for w in WORKLOAD_ORDER if w in values]
    extra = sorted(w for w in values if w not in WORKLOAD_ORDER)
    return known + extra


def place_legend(ax, ncol: int = 2) -> None:
    handles, labels = ax.get_legend_handles_labels()
    if not handles:
        return
    ax.legend(
        handles,
        labels,
        loc="best",
        ncol=ncol,
        frameon=True,
        framealpha=0.88,
        facecolor="white",
        edgecolor="#d9d9d9",
    )


def plot_metric_lines(
    data: pd.DataFrame,
    x: str,
    y: str,
    out_path: Path,
    title: str,
    ylabel: str,
    order: list[int],
    yscale: str | None = None,
) -> None:
    workloads = ordered_workloads(sorted(data["workload"].dropna().unique().tolist()))
    fig, ax = plt.subplots(figsize=(10, 6))
    for workload in workloads:
        sub = data[data["workload"] == workload].sort_values(x)
        if sub.empty:
            continue
        ax.plot(
            sub[x],
            sub[y],
            label=workload,
            color=WORKLOAD_PALETTE.get(workload, None),
            marker=WORKLOAD_MARKERS.get(workload, "o"),
            linestyle=WORKLOAD_LINESTYLES.get(workload, "-"),
            linewidth=2.2,
            markersize=7,
            markeredgecolor="white",
            markeredgewidth=0.8,
        )
    ax.set_title(title)
    ax.set_xlabel("Batch")
    ax.set_ylabel(ylabel)
    ax.set_xticks(order)
    ax.set_xticklabels([str(xv) for xv in order])
    if yscale:
        ax.set_yscale(yscale)
    ax.grid(alpha=0.25)
    place_legend(ax, ncol=2)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_grouped_bars(
    data: pd.DataFrame,
    y: str,
    out_path: Path,
    title: str,
    ylabel: str,
    order: list[int],
    ylim: tuple[float, float] | None = None,
) -> None:
    workloads = ordered_workloads(sorted(data["workload"].dropna().unique().tolist()))
    x = np.arange(len(order), dtype=float)
    width = 0.13 if len(workloads) >= 5 else 0.16

    fig, ax = plt.subplots(figsize=(10.5, 6.2))
    for idx, workload in enumerate(workloads):
        sub = data[data["workload"] == workload].sort_values("batch")
        vals = []
        for batch in order:
            hit = sub[sub["batch"] == batch]
            vals.append(float(hit.iloc[0][y]) if not hit.empty else np.nan)
        offset = (idx - (len(workloads) - 1) / 2) * width
        ax.bar(
            x + offset,
            vals,
            width=width,
            label=workload,
            color=WORKLOAD_PALETTE.get(workload, None),
            edgecolor="white",
            linewidth=0.8,
            alpha=0.92,
        )

    ax.set_title(title)
    ax.set_xlabel("Batch")
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels([str(v) for v in order])
    if ylim is not None:
        ax.set_ylim(*ylim)
    ax.grid(axis="y", alpha=0.25)
    place_legend(ax, ncol=2)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def ensure_dirs(chart_root: Path) -> dict[str, Path]:
    dirs = {
        "root": chart_root,
        "overview": chart_root / "overview",
        "launch_offset": chart_root / "launch_offset",
        "sched": chart_root / "sched",
        "load": chart_root / "load",
        "metrics_root": chart_root / "metrics",
        "metrics_base": chart_root / "metrics" / "base",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


def load_input(data_dir: Path, exclude_workloads: set[str] | None = None) -> pd.DataFrame:
    files = sorted(data_dir.glob("*.csv"))
    if not files:
        raise SystemExit(f"no csv files found in {data_dir}")

    dfs = [pd.read_csv(p) for p in files]
    df = pd.concat(dfs, ignore_index=True)

    required = {"workload", "batch", "start_ts", "elapsed", "sm", "sched"}
    missing = required.difference(df.columns)
    if missing:
        raise SystemExit(f"missing required columns: {sorted(missing)}")
    if "launch_offset" not in df.columns and "delay" not in df.columns:
        raise SystemExit("missing required launch offset column: need launch_offset or delay")

    exclude_workloads = exclude_workloads or set()
    if exclude_workloads:
        df = df[~df["workload"].isin(sorted(exclude_workloads))].copy()
    return df


def compute_launch_offset_metrics(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    work = df.copy()
    offset_col = "launch_offset" if "launch_offset" in work.columns else "delay"
    work["delay_proxy"] = work[offset_col]
    work["launch_offset_proxy"] = work[offset_col]

    work = work.sort_values(["workload", "batch", "start_ts"]).copy()
    work["inter_block_gap"] = work.groupby(["workload", "batch"])["start_ts"].diff().fillna(0)

    summary = (
        work.groupby(["workload", "batch"], as_index=False)
        .agg(
            records=("elapsed", "count"),
            start_min=("start_ts", "min"),
            start_max=("start_ts", "max"),
            delay_mean=("delay_proxy", "mean"),
            delay_p25=("delay_proxy", lambda x: x.quantile(0.25)),
            delay_p50=("delay_proxy", lambda x: x.quantile(0.50)),
            delay_p75=("delay_proxy", lambda x: x.quantile(0.75)),
            delay_p95=("delay_proxy", lambda x: x.quantile(0.95)),
            delay_p99=("delay_proxy", lambda x: x.quantile(0.99)),
            gap_mean=("inter_block_gap", "mean"),
            gap_p95=("inter_block_gap", lambda x: x.quantile(0.95)),
        )
        .sort_values(["workload", "batch"])
    )
    summary["delay_iqr"] = summary["delay_p75"] - summary["delay_p25"]
    summary["delay_tail_amp"] = summary["delay_p99"] / summary["delay_p50"].replace(0, pd.NA)
    summary["delay_tail_amp"] = summary["delay_tail_amp"].fillna(0)
    summary["launch_offset_mean"] = summary["delay_mean"]
    summary["launch_offset_p25"] = summary["delay_p25"]
    summary["launch_offset_p50"] = summary["delay_p50"]
    summary["launch_offset_p75"] = summary["delay_p75"]
    summary["launch_offset_p95"] = summary["delay_p95"]
    summary["launch_offset_p99"] = summary["delay_p99"]
    summary["launch_offset_iqr"] = summary["delay_iqr"]
    summary["launch_offset_tail_amp"] = summary["delay_tail_amp"]

    return work, summary


def compute_sched_metrics(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    work = df.copy()
    work["sched"] = work["sched"].astype("int64")
    work["sched_event"] = work["sched"] > 0

    summary_rows: list[dict] = []
    for (workload, batch), sub in work.groupby(["workload", "batch"], sort=True):
        events = sub[sub["sched"] > 0]["sched"].astype("int64")
        summary_rows.append(
            {
                "workload": workload,
                "batch": int(batch),
                "records": int(len(sub)),
                "sched_event_count": int((sub["sched"] > 0).sum()),
                "sched_event_ratio": float((sub["sched"] > 0).mean()),
                "sched_mean": float(events.mean()) if not events.empty else 0.0,
                "sched_p50": float(events.quantile(0.50)) if not events.empty else 0.0,
                "sched_p75": float(events.quantile(0.75)) if not events.empty else 0.0,
                "sched_p95": float(events.quantile(0.95)) if not events.empty else 0.0,
                "sched_p99": float(events.quantile(0.99)) if not events.empty else 0.0,
                "sched_max": int(events.max()) if not events.empty else 0,
            }
        )

    summary = pd.DataFrame(summary_rows).sort_values(["workload", "batch"]).reset_index(drop=True)
    summary["sched_iqr"] = summary["sched_p75"] - summary["sched_p50"]
    summary["sched_tail_amp"] = summary["sched_p99"] / summary["sched_p50"].replace(0, pd.NA)
    summary["sched_tail_amp"] = summary["sched_tail_amp"].fillna(0)
    return work, summary


def compute_load_metrics(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    per_sm = (
        df.groupby(["workload", "batch", "sm"], as_index=False)
        .agg(block_count=("elapsed", "count"), elapsed_sum=("elapsed", "sum"))
    )

    summary = (
        per_sm.groupby(["workload", "batch"], as_index=False)
        .agg(
            sm_count=("sm", "nunique"),
            block_mean=("block_count", "mean"),
            block_std=("block_count", "std"),
            block_max=("block_count", "max"),
            block_min=("block_count", "min"),
            elapsed_sum_mean=("elapsed_sum", "mean"),
            elapsed_sum_std=("elapsed_sum", "std"),
        )
        .fillna(0)
    )
    summary["block_cv"] = (summary["block_std"] / summary["block_mean"]).replace([float("inf")], 0).fillna(0)
    summary["elapsed_sum_cv"] = (summary["elapsed_sum_std"] / summary["elapsed_sum_mean"]).replace([float("inf")], 0).fillna(0)
    summary["block_imbalance_ratio"] = (
        (summary["block_max"] - summary["block_min"]) / summary["block_mean"].replace(0, pd.NA)
    ).fillna(0)

    fairness = []
    for (workload, batch), sub in per_sm.groupby(["workload", "batch"]):
        vals = sub["block_count"].astype(float)
        n = len(vals)
        denom = n * (vals.pow(2).sum())
        jain = float((vals.sum() ** 2) / denom) if denom > 0 else 0.0
        fairness.append({"workload": workload, "batch": batch, "jain_block_fairness": jain})
    fairness_df = pd.DataFrame(fairness)

    summary = summary.merge(fairness_df, on=["workload", "batch"], how="left")
    summary = summary.sort_values(["workload", "batch"])

    return per_sm, summary


def plot_launch_offset(launch_offset_detail: pd.DataFrame, launch_offset_summary: pd.DataFrame, out_dir: Path) -> None:
    for old_png in out_dir.glob("*.png"):
        old_png.unlink()

    launch_plot = launch_offset_summary[launch_offset_summary["batch"] >= 16].copy()
    if launch_plot.empty:
        launch_plot = launch_offset_summary.copy()

    order = sorted(launch_plot["batch"].dropna().unique().tolist())

    def _apply_batch_ticks(ax) -> None:
        ax.set_xticks(order)
        ax.set_xticklabels([str(x) for x in order])

    plot_metric_lines(
        launch_plot,
        x="batch",
        y="launch_offset_mean",
        out_path=out_dir / "01_launch_offset_mean_by_batch.png",
        title="Relative Launch Offset Mean by Batch (16-128)",
        ylabel="Mean(launch_offset)",
        order=order,
    )

    plot_metric_lines(
        launch_plot,
        x="batch",
        y="launch_offset_p95",
        out_path=out_dir / "02_launch_offset_p95_by_batch.png",
        title="Relative Launch Offset P95 by Batch (16-128)",
        ylabel="P95(launch_offset)",
        order=order,
    )

    plot_metric_lines(
        launch_plot,
        x="batch",
        y="launch_offset_p99",
        out_path=out_dir / "03_launch_offset_p99_by_batch.png",
        title="Relative Launch Offset P99 by Batch (16-128)",
        ylabel="P99(launch_offset)",
        order=order,
    )

    plot_grouped_bars(
        launch_plot,
        y="launch_offset_tail_amp",
        out_path=out_dir / "04_launch_offset_tail_amplification_by_batch.png",
        title="Launch Offset Tail Amplification by Batch (Grouped Bars, 16-128)",
        ylabel="P99 / P50",
        order=order,
    )

    p95_pivot = (
        launch_plot.pivot_table(index="workload", columns="batch", values="launch_offset_p95", aggfunc="mean")
        .reindex(columns=order)
        .fillna(0)
    )
    plt.figure(figsize=(10, 5))
    sns.heatmap(p95_pivot, cmap="YlOrRd", annot=True, fmt=".2f")
    plt.title("Launch Offset P95 Heatmap (Workload x Batch, 16-128)")
    plt.xlabel("Batch")
    plt.ylabel("Workload")
    plt.tight_layout()
    plt.savefig(out_dir / "05_launch_offset_p95_heatmap_workload_batch.png", dpi=180)
    plt.close()

    gap_summary = launch_plot[["workload", "batch", "gap_mean"]].copy()
    workloads = ordered_workloads(sorted(gap_summary["workload"].dropna().unique().tolist()))
    fig, axes = plt.subplots(len(workloads), 1, figsize=(9, 2.5 * len(workloads)), sharex=True)
    if len(workloads) == 1:
        axes = [axes]
    for ax, workload in zip(axes, workloads):
        sub = gap_summary[gap_summary["workload"] == workload].sort_values("batch")
        sns.lineplot(
            data=sub,
            x="batch",
            y="gap_mean",
            marker=WORKLOAD_MARKERS.get(workload, "o"),
            linewidth=2.0,
            color=WORKLOAD_PALETTE.get(workload, "#1f4e79"),
            ax=ax,
        )
        ax.fill_between(sub["batch"], sub["gap_mean"], alpha=0.15, color="#1f4e79")
        ax.set_yscale("log")
        ax.set_title(f"{workload} mean inter-block gap")
        ax.set_ylabel("mean gap")
        _apply_batch_ticks(ax)
        ax.grid(alpha=0.25)
    axes[-1].set_xlabel("Batch")
    fig.suptitle("Inter-block Gap Mean Trends by Workload (16-128)", y=0.995)
    fig.tight_layout()
    fig.savefig(out_dir / "06_inter_block_gap_mean_faceted.png", dpi=180)
    plt.close(fig)

    cdf = launch_offset_detail[launch_offset_detail["batch"] >= 16][["workload", "launch_offset_proxy"]].dropna().copy()
    if cdf.empty:
        cdf = launch_offset_detail[["workload", "launch_offset_proxy"]].dropna().copy()
    if len(cdf) > 250_000:
        cdf = cdf.sample(n=250_000, random_state=42)
    plt.figure(figsize=(10, 6))
    sns.ecdfplot(data=cdf, x="launch_offset_proxy", hue="workload", palette=WORKLOAD_PALETTE, hue_order=ordered_workloads(sorted(cdf["workload"].unique().tolist())))
    plt.title("Launch Offset Empirical CDF by Workload (16-128)")
    plt.xlabel("Launch Offset")
    plt.ylabel("ECDF")
    place_legend(plt.gca(), ncol=2)
    plt.tight_layout()
    plt.savefig(out_dir / "07_launch_offset_ecdf_by_workload.png", dpi=180)
    plt.close()

    quantile_plot = launch_plot[
        ["workload", "batch", "launch_offset_p50", "launch_offset_p95", "launch_offset_p99"]
    ].copy()
    workloads = ordered_workloads(sorted(quantile_plot["workload"].dropna().unique().tolist()))
    fig, axes = plt.subplots(len(workloads), 1, figsize=(9, 2.7 * len(workloads)), sharex=True)
    if len(workloads) == 1:
        axes = [axes]
    for ax, workload in zip(axes, workloads):
        sub = quantile_plot[quantile_plot["workload"] == workload].sort_values("batch")
        ax.plot(sub["batch"], sub["launch_offset_p50"], marker="o", linewidth=1.5, color="#6c757d", label="P50")
        ax.plot(sub["batch"], sub["launch_offset_p95"], marker="o", linewidth=2.0, color="#ff7f0e", label="P95")
        ax.plot(sub["batch"], sub["launch_offset_p99"], marker="o", linewidth=2.0, color="#d62728", label="P99")
        ax.fill_between(sub["batch"], sub["launch_offset_p50"], sub["launch_offset_p95"], alpha=0.12, color="#ffbe7d")
        ax.fill_between(sub["batch"], sub["launch_offset_p95"], sub["launch_offset_p99"], alpha=0.10, color="#f28e8c")
        ax.set_title(f"{workload} launch offset quantile ladder")
        ax.set_ylabel("launch offset")
        _apply_batch_ticks(ax)
        ax.grid(alpha=0.25)
    place_legend(axes[0], ncol=3)
    axes[-1].set_xlabel("Batch")
    fig.suptitle("Launch Offset Quantile Trends by Workload (16-128)", y=0.995)
    fig.tight_layout()
    fig.savefig(out_dir / "08_launch_offset_quantile_ladders_by_workload.png", dpi=180)
    plt.close(fig)

    plot_grouped_bars(
        launch_plot,
        y="launch_offset_iqr",
        out_path=out_dir / "09_launch_offset_iqr_by_batch.png",
        title="Launch Offset Dispersion (IQR) by Batch (Grouped Bars, 16-128)",
        ylabel="IQR(launch_offset)",
        order=order,
    )

    growth = launch_plot[["workload", "batch", "launch_offset_p95"]].copy()
    growth["baseline_p95"] = growth.groupby("workload")["launch_offset_p95"].transform("min").replace(0, pd.NA)
    growth["launch_offset_p95_growth"] = (growth["launch_offset_p95"] / growth["baseline_p95"]).fillna(0)
    plot_metric_lines(
        growth,
        x="batch",
        y="launch_offset_p95_growth",
        out_path=out_dir / "10_launch_offset_p95_growth_vs_baseline.png",
        title="Launch Offset P95 Growth Relative to Each Workload Baseline (16-128)",
        ylabel="P95 / min(P95)",
        order=order,
    )

    plot_grouped_bars(
        launch_plot,
        y="launch_offset_p95",
        out_path=out_dir / "11_launch_offset_p95_grouped_bar_by_batch.png",
        title="Launch Offset P95 by Batch (Grouped Bars, 16-128)",
        ylabel="P95(launch_offset)",
        order=order,
    )

    plot_grouped_bars(
        launch_plot,
        y="gap_mean",
        out_path=out_dir / "12_inter_block_gap_mean_grouped_bar_by_batch.png",
        title="Inter-block Gap Mean by Batch (Grouped Bars, 16-128)",
        ylabel="mean gap",
        order=order,
    )


def plot_sched(sched_detail: pd.DataFrame, sched_summary: pd.DataFrame, out_dir: Path) -> None:
    for old_png in out_dir.glob("*.png"):
        old_png.unlink()

    sched_plot = sched_summary[sched_summary["batch"] >= 16].copy()
    if sched_plot.empty:
        sched_plot = sched_summary.copy()

    order = sorted(sched_plot["batch"].dropna().unique().tolist())

    def _apply_batch_ticks(ax) -> None:
        ax.set_xticks(order)
        ax.set_xticklabels([str(x) for x in order])

    plot_metric_lines(
        sched_plot,
        x="batch",
        y="sched_mean",
        out_path=out_dir / "01_sched_mean_by_batch.png",
        title="Sched Mean by Batch (16-128)",
        ylabel="Mean(sched)",
        order=order,
    )

    plot_metric_lines(
        sched_plot,
        x="batch",
        y="sched_p95",
        out_path=out_dir / "02_sched_p95_by_batch.png",
        title="Sched P95 by Batch (16-128)",
        ylabel="P95(sched)",
        order=order,
    )

    plot_grouped_bars(
        sched_plot,
        y="sched_event_ratio",
        out_path=out_dir / "03_sched_event_ratio_by_batch.png",
        title="Sched Event Ratio by Batch (Grouped Bars, 16-128)",
        ylabel="event ratio",
        order=order,
        ylim=(0.0, 1.0),
    )

    plot_metric_lines(
        sched_plot,
        x="batch",
        y="sched_max",
        out_path=out_dir / "04_sched_max_by_batch.png",
        title="Sched Max by Batch (16-128, log scale)",
        ylabel="max(sched)",
        order=order,
        yscale="log",
    )

    p95_pivot = (
        sched_plot.pivot_table(index="workload", columns="batch", values="sched_p95", aggfunc="mean")
        .reindex(columns=order)
        .fillna(0)
    )
    plt.figure(figsize=(10, 5))
    sns.heatmap(p95_pivot, cmap="YlOrRd", annot=True, fmt=".2f")
    plt.title("Sched P95 Heatmap (Workload x Batch, 16-128)")
    plt.xlabel("Batch")
    plt.ylabel("Workload")
    plt.tight_layout()
    plt.savefig(out_dir / "05_sched_p95_heatmap_workload_batch.png", dpi=180)
    plt.close()

    cdf = sched_detail[sched_detail["sched"] > 0][["workload", "sched"]].dropna().copy()
    if len(cdf) > 250_000:
        cdf = cdf.sample(n=250_000, random_state=42)
    plt.figure(figsize=(10, 6))
    if cdf.empty:
        plt.text(0.5, 0.5, "No non-zero sched events", ha="center", va="center")
        plt.xlim(0, 1)
        plt.ylim(0, 1)
    else:
        sns.ecdfplot(data=cdf, x="sched", hue="workload")
    plt.title("Sched Empirical CDF by Workload")
    plt.xlabel("sched")
    plt.ylabel("ECDF")
    plt.tight_layout()
    plt.savefig(out_dir / "06_sched_ecdf_by_workload.png", dpi=180)
    plt.close()

    quantile_plot = sched_plot[["workload", "batch", "sched_p50", "sched_p95", "sched_p99"]].copy()
    workloads = sorted(quantile_plot["workload"].dropna().unique().tolist())
    fig, axes = plt.subplots(len(workloads), 1, figsize=(9, 2.7 * len(workloads)), sharex=True)
    if len(workloads) == 1:
        axes = [axes]
    for ax, workload in zip(axes, ordered_workloads(workloads)):
        sub = quantile_plot[quantile_plot["workload"] == workload].sort_values("batch")
        ax.plot(sub["batch"], sub["sched_p50"], marker="o", linewidth=1.5, color="#6c757d", label="P50")
        ax.plot(sub["batch"], sub["sched_p95"], marker="o", linewidth=2.0, color="#ff7f0e", label="P95")
        ax.plot(sub["batch"], sub["sched_p99"], marker="o", linewidth=2.0, color="#d62728", label="P99")
        ax.fill_between(sub["batch"], sub["sched_p50"], sub["sched_p95"], alpha=0.12, color="#ffbe7d")
        ax.fill_between(sub["batch"], sub["sched_p95"], sub["sched_p99"], alpha=0.10, color="#f28e8c")
        ax.set_title(f"{workload} sched quantile ladder")
        ax.set_ylabel("sched")
        _apply_batch_ticks(ax)
        ax.grid(alpha=0.25)
    place_legend(axes[0], ncol=3)
    axes[-1].set_xlabel("Batch")
    fig.suptitle("Sched Quantile Trends by Workload (16-128)", y=0.995)
    fig.tight_layout()
    fig.savefig(out_dir / "07_sched_quantile_ladders_by_workload.png", dpi=180)
    plt.close(fig)

    plot_grouped_bars(
        sched_plot,
        y="sched_p95",
        out_path=out_dir / "08_sched_p95_grouped_bar_by_batch.png",
        title="Sched P95 by Batch (Grouped Bars, 16-128)",
        ylabel="P95(sched)",
        order=order,
    )

    plot_grouped_bars(
        sched_plot,
        y="sched_mean",
        out_path=out_dir / "09_sched_mean_grouped_bar_by_batch.png",
        title="Sched Mean by Batch (Grouped Bars, 16-128)",
        ylabel="Mean(sched)",
        order=order,
    )


def plot_load(per_sm: pd.DataFrame, load_summary: pd.DataFrame, out_dir: Path) -> None:
    load_plot = load_summary[load_summary["batch"] >= 16].copy()
    if load_plot.empty:
        load_plot = load_summary.copy()

    per_sm_plot = per_sm[per_sm["batch"] >= 16].copy()
    if per_sm_plot.empty:
        per_sm_plot = per_sm.copy()

    order = sorted(load_plot["batch"].dropna().unique().tolist())

    def _apply_batch_ticks(ax) -> None:
        ax.set_xticks(order)
        ax.set_xticklabels([str(x) for x in order])

    plot_metric_lines(
        load_plot,
        x="batch",
        y="block_imbalance_ratio",
        out_path=out_dir / "01_block_imbalance_ratio_by_batch.png",
        title="Block Imbalance Ratio by Batch (16-128)",
        ylabel="(max-min)/mean block_count across SM",
        order=order,
    )

    plot_metric_lines(
        load_plot,
        x="batch",
        y="elapsed_sum_cv",
        out_path=out_dir / "02_elapsed_sum_cv_by_batch.png",
        title="Elapsed Sum CV by Batch (16-128)",
        ylabel="CV(elapsed_sum across SM)",
        order=order,
    )

    plot_grouped_bars(
        load_plot,
        y="jain_block_fairness",
        out_path=out_dir / "03_jain_fairness_by_batch.png",
        title="Jain Fairness by Batch (Grouped Bars, 16-128)",
        ylabel="Jain index (1 is most balanced)",
        order=order,
        ylim=(max(0.0, float(load_plot["jain_block_fairness"].min()) - 0.02), 1.001),
    )

    for workload in ordered_workloads(sorted(per_sm_plot["workload"].unique().tolist())):
        sub = per_sm_plot[per_sm_plot["workload"] == workload]
        pivot_blocks = sub.pivot_table(index="batch", columns="sm", values="block_count", aggfunc="sum", fill_value=0)
        pivot_elapsed = sub.pivot_table(index="batch", columns="sm", values="elapsed_sum", aggfunc="sum", fill_value=0)

        plt.figure(figsize=(12, 5))
        sns.heatmap(pivot_blocks, cmap="YlGnBu")
        plt.title(f"SM Block Count Heatmap - {workload}")
        plt.xlabel("SM")
        plt.ylabel("Batch")
        plt.tight_layout()
        plt.savefig(out_dir / f"04_sm_block_heatmap_{workload}.png", dpi=180)
        plt.close()

        plt.figure(figsize=(12, 5))
        sns.heatmap(pivot_elapsed, cmap="OrRd")
        plt.title(f"SM Elapsed Sum Heatmap - {workload}")
        plt.xlabel("SM")
        plt.ylabel("Batch")
        plt.tight_layout()
        plt.savefig(out_dir / f"05_sm_elapsed_heatmap_{workload}.png", dpi=180)
        plt.close()

    plot_grouped_bars(
        load_plot,
        y="block_imbalance_ratio",
        out_path=out_dir / "06_block_imbalance_ratio_grouped_bar_by_batch.png",
        title="Block Imbalance Ratio by Batch (Grouped Bars, 16-128)",
        ylabel="(max-min)/mean block_count across SM",
        order=order,
    )

    plot_grouped_bars(
        load_plot,
        y="elapsed_sum_cv",
        out_path=out_dir / "07_elapsed_sum_cv_grouped_bar_by_batch.png",
        title="Elapsed Sum CV by Batch (Grouped Bars, 16-128)",
        ylabel="CV(elapsed_sum across SM)",
        order=order,
    )


def plot_overview(df: pd.DataFrame, out_dir: Path) -> None:
    overview_df = df[df["batch"] >= 16].copy()
    if overview_df.empty:
        overview_df = df.copy()

    plt.figure(figsize=(10, 6))
    sns.histplot(
        data=overview_df,
        x="elapsed",
        hue="workload",
        bins=100,
        stat="density",
        element="step",
        common_norm=False,
        palette=WORKLOAD_PALETTE,
        hue_order=ordered_workloads(sorted(overview_df["workload"].dropna().unique().tolist())),
    )
    plt.title("Elapsed Distribution by Workload (16-128)")
    plt.xlabel("Elapsed")
    plt.ylabel("Density")
    place_legend(plt.gca(), ncol=2)
    plt.tight_layout()
    plt.savefig(out_dir / "01_elapsed_distribution_by_workload.png", dpi=180)
    plt.close()

    elapsed_summary = (
        overview_df.groupby(["workload", "batch"], as_index=False)
        .agg(elapsed_mean=("elapsed", "mean"))
        .sort_values(["workload", "batch"])
    )
    order = sorted(elapsed_summary["batch"].dropna().unique().tolist())
    plot_grouped_bars(
        elapsed_summary,
        y="elapsed_mean",
        out_path=out_dir / "02_elapsed_mean_grouped_bar_by_batch.png",
        title="Elapsed Mean by Batch (Grouped Bars, 16-128)",
        ylabel="Mean(elapsed)",
        order=order,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze block-level CSV files and generate charts")
    parser.add_argument("--data-dir", default="../output/data", help="input data directory containing per-workload csv")
    parser.add_argument("--chart-dir", default="../output/chart", help="chart output directory")
    parser.add_argument(
        "--exclude-workloads",
        nargs="*",
        default=[],
        help="workload names excluded from charts and base summaries",
    )
    args = parser.parse_args()

    data_dir = (BASE_DIR / args.data_dir).resolve() if not Path(args.data_dir).is_absolute() else Path(args.data_dir)
    exclude = {w.strip() for w in args.exclude_workloads if w and w.strip()}
    df = load_input(data_dir, exclude_workloads=exclude)
    chart_dir = (BASE_DIR / args.chart_dir).resolve() if not Path(args.chart_dir).is_absolute() else Path(args.chart_dir)
    dirs = ensure_dirs(chart_dir)

    launch_offset_detail, launch_offset_summary = compute_launch_offset_metrics(df)
    sched_detail, sched_summary = compute_sched_metrics(df)
    per_sm, load_summary = compute_load_metrics(df)

    launch_offset_detail.to_csv(dirs["metrics_base"] / "delay_detail.csv", index=False)
    launch_offset_summary.to_csv(dirs["metrics_base"] / "delay_summary_by_workload_batch.csv", index=False)
    launch_offset_summary_out = launch_offset_summary[
        [
            "workload",
            "batch",
            "records",
            "start_min",
            "start_max",
            "launch_offset_mean",
            "launch_offset_p25",
            "launch_offset_p50",
            "launch_offset_p75",
            "launch_offset_p95",
            "launch_offset_p99",
            "gap_mean",
            "gap_p95",
            "launch_offset_iqr",
            "launch_offset_tail_amp",
        ]
    ].copy()
    launch_offset_detail.to_csv(dirs["metrics_base"] / "launch_offset_detail.csv", index=False)
    launch_offset_summary_out.to_csv(dirs["metrics_base"] / "launch_offset_summary_by_workload_batch.csv", index=False)
    sched_detail.to_csv(dirs["metrics_base"] / "sched_block_detail.csv", index=False)
    sched_summary.to_csv(dirs["metrics_base"] / "sched_block_summary_by_workload_batch.csv", index=False)
    per_sm.to_csv(dirs["metrics_base"] / "load_per_sm.csv", index=False)
    load_summary.to_csv(dirs["metrics_base"] / "load_summary_by_workload_batch.csv", index=False)

    plot_launch_offset(launch_offset_detail, launch_offset_summary, dirs["launch_offset"])
    plot_sched(sched_detail, sched_summary, dirs["sched"])
    plot_load(per_sm, load_summary, dirs["load"])
    plot_overview(df, dirs["overview"])

    print(f"[done] chart dir: {dirs['root']}")


if __name__ == "__main__":
    main()
