#!/usr/bin/env python3
"""Analyze per-workload block CSV files and generate metrics/charts.

Input layout:
    output/data/*.csv

Output layout:
    output/chart/{overview,sched,load,correlation}/*.png
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
from matplotlib.lines import Line2D

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


def bbox_intersection_area(a, b) -> float:
    x0 = max(a.x0, b.x0)
    y0 = max(a.y0, b.y0)
    x1 = min(a.x1, b.x1)
    y1 = min(a.y1, b.y1)
    if x1 <= x0 or y1 <= y0:
        return 0.0
    return float((x1 - x0) * (y1 - y0))


def place_relation_annotations(
    fig,
    ax: plt.Axes,
    annotation_specs: list[tuple[float, float, int, str, str]],
    fontsize: float,
    candidates: list[tuple[int, int]] | None = None,
    preferred_offsets_by_batch: dict[int, list[tuple[int, int]]] | None = None,
    sort_key=None,
) -> None:
    renderer = fig.canvas.get_renderer()
    axes_bbox = ax.get_window_extent(renderer).padded(-4.0)
    placed_bboxes = []
    placed_by_workload: dict[str, list[tuple[float, tuple[float, float, float, float]]]] = {}
    base_candidates = candidates or [
        (6, 6),
        (-18, 6),
        (6, -10),
        (-18, -10),
        (0, 10),
        (10, 0),
        (-18, 0),
        (0, -12),
        (12, 12),
        (-22, 12),
        (12, -16),
        (-22, -16),
    ]

    ordered_specs = list(annotation_specs)
    if sort_key is not None:
        ordered_specs = sorted(ordered_specs, key=sort_key)

    for x, y, batch, color, workload in ordered_specs:
        local_candidates = []
        if preferred_offsets_by_batch:
            local_candidates.extend(preferred_offsets_by_batch.get(int(batch), []))
        local_candidates.extend(base_candidates)
        dedup_candidates = list(dict.fromkeys(local_candidates))
        best_score = None
        best_offset = dedup_candidates[0]

        for dx, dy in dedup_candidates:
            text = ax.annotate(
                f"b{batch}",
                (x, y),
                textcoords="offset points",
                xytext=(dx, dy),
                fontsize=fontsize,
                color=color,
            )
            bbox = text.get_window_extent(renderer=renderer).expanded(1.06, 1.15)
            text.remove()

            overlap_penalty = sum(bbox_intersection_area(bbox, other) for other in placed_bboxes)
            outside_penalty = 0.0
            if bbox.x0 < axes_bbox.x0:
                outside_penalty += axes_bbox.x0 - bbox.x0
            if bbox.x1 > axes_bbox.x1:
                outside_penalty += bbox.x1 - axes_bbox.x1
            if bbox.y0 < axes_bbox.y0:
                outside_penalty += axes_bbox.y0 - bbox.y0
            if bbox.y1 > axes_bbox.y1:
                outside_penalty += bbox.y1 - axes_bbox.y1

            distance_penalty = abs(dx) + abs(dy)
            order_penalty = 0.0
            for other_x, other_bbox_vals in placed_by_workload.get(workload, []):
                other_center_x = (other_bbox_vals[0] + other_bbox_vals[2]) / 2.0
                this_center_x = (bbox.x0 + bbox.x1) / 2.0
                if x > other_x and this_center_x < other_center_x - 2.0:
                    order_penalty += other_center_x - this_center_x
                elif x < other_x and this_center_x > other_center_x + 2.0:
                    order_penalty += this_center_x - other_center_x

            score = (
                outside_penalty * 1000.0
                + overlap_penalty * 100.0
                + order_penalty * 25.0
                + distance_penalty
            )
            if best_score is None or score < best_score:
                best_score = score
                best_offset = (dx, dy)

        final_text = ax.annotate(
            f"b{batch}",
            (x, y),
            textcoords="offset points",
            xytext=best_offset,
            fontsize=fontsize,
            color=color,
        )
        placed_bboxes.append(final_text.get_window_extent(renderer=renderer).expanded(1.06, 1.15))
        final_bbox = final_text.get_window_extent(renderer=renderer).expanded(1.06, 1.15)
        placed_by_workload.setdefault(workload, []).append(
            (x, (final_bbox.x0, final_bbox.y0, final_bbox.x1, final_bbox.y1))
        )


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
        "sched": chart_root / "sched",
        "load": chart_root / "load",
        "correlation": chart_root / "correlation",
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

    exclude_workloads = exclude_workloads or set()
    if exclude_workloads:
        df = df[~df["workload"].isin(sorted(exclude_workloads))].copy()
    return df


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
        ylabel="Sched Mean (cycles)",
        order=order,
    )

    plot_metric_lines(
        sched_plot,
        x="batch",
        y="sched_p95",
        out_path=out_dir / "02_sched_p95_by_batch.png",
        title="Sched P95 by Batch (16-128)",
        ylabel="Sched P95 (cycles)",
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
        ylabel="Sched Max (cycles)",
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
    plt.xlabel("Sched (cycles)")
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
        ax.set_title(f"{workload} Sched Quantile Ladder")
        ax.set_ylabel("Sched (cycles)")
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
        ylabel="Sched P95 (cycles)",
        order=order,
    )

    plot_grouped_bars(
        sched_plot,
        y="sched_mean",
        out_path=out_dir / "09_sched_mean_grouped_bar_by_batch.png",
        title="Sched Mean by Batch (Grouped Bars, 16-128)",
        ylabel="Sched Mean (cycles)",
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


def _plot_relation_panel(
    corr_plot: pd.DataFrame,
    out_path: Path,
    y_col: str,
    y_label: str,
    fig_title: str,
    panel_title_prefix: str,
    yscale: str | None = None,
) -> None:
    metrics = [
        (
            "block_imbalance_ratio",
            "Block imbalance ratio",
            f"{panel_title_prefix} vs Block Imbalance Ratio",
        ),
        (
            "elapsed_sum_cv",
            "Elapsed sum CV",
            f"{panel_title_prefix} vs Elapsed Sum CV",
        ),
        (
            "jain_block_fairness",
            "Jain block fairness",
            f"{panel_title_prefix} vs Jain Fairness",
        ),
    ]

    workloads = ordered_workloads(sorted(corr_plot["workload"].dropna().unique().tolist()))
    fig, axes = plt.subplots(1, len(metrics), figsize=(18.8, 7.4), sharey=True)
    pending_annotations: list[tuple[plt.Axes, list[tuple[float, float, int, str, str]]]] = []

    for ax, (x_col, xlabel, title) in zip(axes, metrics):
        x_vals = corr_plot[x_col].to_numpy(dtype=float)
        y_vals = corr_plot[y_col].to_numpy(dtype=float)
        valid = np.isfinite(x_vals) & np.isfinite(y_vals)
        axis_annotations: list[tuple[float, float, int, str, str]] = []
        if valid.sum() >= 2 and np.nanmin(x_vals[valid]) != np.nanmax(x_vals[valid]):
            coeffs = np.polyfit(x_vals[valid], y_vals[valid], deg=1)
            xs = np.linspace(float(np.nanmin(x_vals[valid])), float(np.nanmax(x_vals[valid])), 100)
            ax.plot(
                xs,
                coeffs[0] * xs + coeffs[1],
                color="#7f7f7f",
                linestyle="--",
                linewidth=1.4,
                alpha=0.85,
            )

        for workload in workloads:
            sub = corr_plot[corr_plot["workload"] == workload].sort_values("batch")
            if sub.empty:
                continue
            ax.plot(
                sub[x_col],
                sub[y_col],
                color=WORKLOAD_PALETTE.get(workload, None),
                alpha=0.28,
                linewidth=1.0,
            )
            ax.scatter(
                sub[x_col],
                sub[y_col],
                label=workload,
                color=WORKLOAD_PALETTE.get(workload, None),
                marker=WORKLOAD_MARKERS.get(workload, "o"),
                s=64,
                edgecolor="white",
                linewidth=0.8,
                alpha=0.95,
            )
            for _, row in sub.iterrows():
                axis_annotations.append(
                    (
                        float(row[x_col]),
                        float(row[y_col]),
                        int(row["batch"]),
                        WORKLOAD_PALETTE.get(workload, "#333333"),
                        workload,
                    )
                )

        corr_matrix = corr_plot[[x_col, y_col]].corr(method="pearson")
        pearson = float(corr_matrix.iloc[0, 1]) if corr_matrix.shape == (2, 2) else float("nan")
        rank_matrix = corr_plot[[x_col, y_col]].corr(method="spearman")
        spearman = float(rank_matrix.iloc[0, 1]) if rank_matrix.shape == (2, 2) else float("nan")
        ax.set_title(f"{title}\nPearson={pearson:.2f}, Spearman={spearman:.2f}", pad=12, fontsize=12.8)
        ax.set_xlabel(xlabel, fontsize=11.8)
        ax.grid(alpha=0.25)
        ax.tick_params(axis="both", labelsize=10.9)
        if yscale:
            ax.set_yscale(yscale)
        pending_annotations.append((ax, axis_annotations))

    fig.canvas.draw()
    for ax, axis_annotations in pending_annotations:
        if "Jain Fairness" in ax.get_title():
            place_relation_annotations(
                fig,
                ax,
                axis_annotations,
                fontsize=9.2,
                candidates=[
                    (-20, 8),
                    (-24, -4),
                    (-28, 16),
                    (-28, -14),
                    (-14, 14),
                    (-14, -10),
                    (-34, 6),
                    (-34, -8),
                    (8, 6),
                    (8, -10),
                ],
                preferred_offsets_by_batch={
                    16: [(-16, 8), (-20, -2)],
                    32: [(-18, 16), (-24, 4)],
                    64: [(-18, -10), (-24, -2)],
                    128: [(-24, 22), (-30, 10)],
                },
                sort_key=lambda spec: (-spec[0], spec[1]),
            )
        else:
            place_relation_annotations(fig, ax, axis_annotations, fontsize=9.2)

    axes[0].set_ylabel(y_label, fontsize=11.8)
    legend_handles = []
    legend_labels = []
    for workload in workloads:
        legend_handles.append(
            Line2D(
                [0],
                [0],
                color=WORKLOAD_PALETTE.get(workload, "#333333"),
                linestyle="-",
                linewidth=1.4,
                marker=WORKLOAD_MARKERS.get(workload, "o"),
                markersize=6.8,
                markeredgecolor="white",
                markeredgewidth=0.8,
                markerfacecolor=WORKLOAD_PALETTE.get(workload, "#333333"),
            )
        )
        legend_labels.append(workload)

    style_handles = [
        Line2D(
            [0],
            [0],
            color="#7f7f7f",
            linestyle="--",
            linewidth=1.4,
            label="correlation trend",
        ),
        Line2D(
            [0],
            [0],
            color="none",
            linewidth=0.0,
            label="b16/b32/b64/b128: batch size",
        ),
    ]
    legend_handles.extend(style_handles)
    legend_labels.extend([handle.get_label() for handle in style_handles])
    fig.legend(
        legend_handles,
        legend_labels,
        loc="lower center",
        ncol=len(legend_labels),
        frameon=True,
        framealpha=0.92,
        facecolor="white",
        edgecolor="#d9d9d9",
        fontsize=10.6,
        handlelength=1.45,
        handletextpad=0.34,
        columnspacing=0.55,
        borderpad=0.28,
        bbox_to_anchor=(0.5, 0.02),
    )
    fig.suptitle(fig_title, y=0.985, fontsize=15.0)
    fig.tight_layout(rect=(0.02, 0.12, 0.98, 0.93))
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_sched_load_correlation(
    sched_summary: pd.DataFrame,
    load_summary: pd.DataFrame,
    out_dir: Path,
) -> pd.DataFrame:
    for old_png in out_dir.glob("*.png"):
        old_png.unlink()

    corr_df = sched_summary.merge(load_summary, on=["workload", "batch"], how="inner")
    corr_plot = corr_df[corr_df["batch"] >= 16].copy()
    if corr_plot.empty:
        corr_plot = corr_df.copy()
    if corr_plot.empty:
        return corr_df

    _plot_relation_panel(
        corr_plot,
        out_dir / "01_sched_p95_vs_load_metrics.png",
        y_col="sched_p95",
        y_label="Sched P95 (cycles)",
        fig_title="Sched P95 to Load Metrics Relation (16-128)",
        panel_title_prefix="Sched P95",
    )
    _plot_relation_panel(
        corr_plot,
        out_dir / "02_sched_mean_vs_load_metrics.png",
        y_col="sched_mean",
        y_label="Sched Mean (cycles)",
        fig_title="Sched Mean to Load Metrics Relation (16-128)",
        panel_title_prefix="Sched Mean",
    )
    _plot_relation_panel(
        corr_plot,
        out_dir / "03_sched_max_vs_load_metrics.png",
        y_col="sched_max",
        y_label="Sched Max (cycles, log scale)",
        fig_title="Sched Max to Load Metrics Relation (16-128)",
        panel_title_prefix="Sched Max",
        yscale="log",
    )
    _plot_relation_panel(
        corr_plot,
        out_dir / "04_sched_event_ratio_vs_load_metrics.png",
        y_col="sched_event_ratio",
        y_label="Sched Event Ratio",
        fig_title="Sched Event Ratio to Load Metrics Relation (16-128)",
        panel_title_prefix="Sched Event Ratio",
    )

    return corr_df


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

    sched_detail, sched_summary = compute_sched_metrics(df)
    per_sm, load_summary = compute_load_metrics(df)
    corr_summary = sched_summary.merge(load_summary, on=["workload", "batch"], how="inner")

    sched_detail.to_csv(dirs["metrics_base"] / "sched_block_detail.csv", index=False)
    sched_summary.to_csv(dirs["metrics_base"] / "sched_block_summary_by_workload_batch.csv", index=False)
    per_sm.to_csv(dirs["metrics_base"] / "load_per_sm.csv", index=False)
    load_summary.to_csv(dirs["metrics_base"] / "load_summary_by_workload_batch.csv", index=False)
    corr_summary.to_csv(dirs["metrics_base"] / "sched_load_relation_by_workload_batch.csv", index=False)

    plot_sched(sched_detail, sched_summary, dirs["sched"])
    plot_load(per_sm, load_summary, dirs["load"])
    plot_overview(df, dirs["overview"])
    plot_sched_load_correlation(sched_summary, load_summary, dirs["correlation"])

    print(f"[done] chart dir: {dirs['root']}")


if __name__ == "__main__":
    main()
