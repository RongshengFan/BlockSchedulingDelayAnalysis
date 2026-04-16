#!/usr/bin/env python3
"""Ablation analysis for workload risk ranking.

This module computes ranking changes under workload-exclusion scenarios without
altering existing parsing or plotting logic.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent


def load_metric_tables(metrics_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    sched_file = metrics_dir / "sched_summary_by_workload_batch.csv"
    load_file = metrics_dir / "load_summary_by_workload_batch.csv"
    if not sched_file.exists():
        raise SystemExit(f"missing file: {sched_file}")
    if not load_file.exists():
        raise SystemExit(f"missing file: {load_file}")

    delay = pd.read_csv(sched_file)
    load = pd.read_csv(load_file)
    req_delay = {"workload", "batch", "sched_p95_cycles"}
    req_load = {"workload", "batch", "block_imbalance_ratio", "elapsed_sum_cv", "jain_block_fairness"}
    md = req_delay.difference(delay.columns)
    ml = req_load.difference(load.columns)
    if md:
        raise SystemExit(f"missing columns in delay metrics: {sorted(md)}")
    if ml:
        raise SystemExit(f"missing columns in load metrics: {sorted(ml)}")
    return delay, load


def aggregate_ranking(delay_df: pd.DataFrame, load_df: pd.DataFrame) -> pd.DataFrame:
    delay_rank = (
        delay_df.groupby("workload", as_index=False)["sched_p95_cycles"]
        .mean()
        .rename(columns={"sched_p95_cycles": "sched_p95_mean"})
    )
    load_rank = (
        load_df.groupby("workload", as_index=False)[["block_imbalance_ratio", "elapsed_sum_cv", "jain_block_fairness"]]
        .mean()
        .rename(
            columns={
                "block_imbalance_ratio": "imbalance_mean",
                "elapsed_sum_cv": "elapsed_cv_mean",
                "jain_block_fairness": "jain_mean",
            }
        )
    )

    rank = delay_rank.merge(load_rank, on="workload", how="outer").fillna(0)
    # Higher scheduling-gap/imbalance/CV are worse, lower Jain fairness is worse.
    # Risk score is defined so larger score means higher risk.
    rank["rank_sched"] = rank["sched_p95_mean"].rank(ascending=True, method="min")
    rank["rank_imbalance"] = rank["imbalance_mean"].rank(ascending=True, method="min")
    rank["rank_cv"] = rank["elapsed_cv_mean"].rank(ascending=True, method="min")
    rank["rank_fairness_bad"] = rank["jain_mean"].rank(ascending=False, method="min")
    rank["overall_risk_score"] = rank[["rank_sched", "rank_imbalance", "rank_cv", "rank_fairness_bad"]].sum(axis=1)
    return rank.sort_values(["overall_risk_score", "workload"], ascending=[False, True]).reset_index(drop=True)


def run_exclusion_scenarios(
    delay_df: pd.DataFrame,
    load_df: pd.DataFrame,
    exclude_candidates: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    base = aggregate_ranking(delay_df, load_df)
    if base.empty:
        raise SystemExit("cannot run ablation on empty ranking")

    rows: list[dict] = []
    details: list[pd.DataFrame] = []

    workloads = sorted(base["workload"].astype(str).tolist())
    for ex in exclude_candidates:
        if ex not in workloads:
            rows.append(
                {
                    "scenario": f"exclude_{ex}",
                    "excluded": ex,
                    "valid": False,
                    "reason": "workload_not_in_base",
                    "top_workload": "",
                    "top_score": 0.0,
                    "avg_score": 0.0,
                    "n_workloads": len(workloads),
                }
            )
            continue

        d = delay_df[delay_df["workload"] != ex].copy()
        l = load_df[load_df["workload"] != ex].copy()
        rank = aggregate_ranking(d, l)
        if rank.empty:
            rows.append(
                {
                    "scenario": f"exclude_{ex}",
                    "excluded": ex,
                    "valid": False,
                    "reason": "empty_after_exclusion",
                    "top_workload": "",
                    "top_score": 0.0,
                    "avg_score": 0.0,
                    "n_workloads": 0,
                }
            )
            continue

        top = rank.iloc[0]
        rows.append(
            {
                "scenario": f"exclude_{ex}",
                "excluded": ex,
                "valid": True,
                "reason": "",
                "top_workload": str(top["workload"]),
                "top_score": float(top["overall_risk_score"]),
                "avg_score": float(rank["overall_risk_score"].mean()),
                "n_workloads": int(rank["workload"].nunique()),
            }
        )
        rank = rank.copy()
        rank.insert(0, "scenario", f"exclude_{ex}")
        details.append(rank)

    summary = pd.DataFrame(rows)
    detail_df = pd.concat(details, ignore_index=True) if details else pd.DataFrame()
    return summary, detail_df


def write_outputs(summary: pd.DataFrame, details: pd.DataFrame, out_dir: Path) -> tuple[Path, Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_csv = out_dir / "ablation_exclusion_summary.csv"
    detail_csv = out_dir / "ablation_exclusion_detail.csv"
    md = out_dir / "ablation_exclusion_summary.md"

    summary.to_csv(summary_csv, index=False)
    details.to_csv(detail_csv, index=False)

    lines: list[str] = []
    lines.append("# Ablation Exclusion Summary")
    lines.append("")
    if summary.empty:
        lines.append("- no scenario results")
    else:
        for _, row in summary.iterrows():
            if bool(row.get("valid", False)):
                lines.append(
                    "- "
                    f"{row['scenario']}: top={row['top_workload']}, "
                    f"top_score={float(row['top_score']):.2f}, "
                    f"avg_score={float(row['avg_score']):.2f}, "
                    f"n_workloads={int(row['n_workloads'])}"
                )
            else:
                lines.append(f"- {row['scenario']}: invalid ({row['reason']})")
        lines.append("")

    md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary_csv, detail_csv, md


def main() -> None:
    parser = argparse.ArgumentParser(description="Run workload exclusion ablation on existing metric tables")
    parser.add_argument("--metrics-dir", default="../output/chart/metrics/base", help="metrics directory from analyze/report steps")
    parser.add_argument("--out-dir", default="../output/chart/metrics/ablation", help="output directory for ablation artifacts")
    parser.add_argument(
        "--exclude-candidates",
        nargs="*",
        default=["compute", "memory", "mixed", "sparse", "vgg16"],
        help="workloads to exclude one-by-one for sensitivity analysis",
    )
    args = parser.parse_args()

    metrics_dir = (BASE_DIR / args.metrics_dir).resolve() if not Path(args.metrics_dir).is_absolute() else Path(args.metrics_dir)
    out_dir = (BASE_DIR / args.out_dir).resolve() if not Path(args.out_dir).is_absolute() else Path(args.out_dir)

    delay_df, load_df = load_metric_tables(metrics_dir)
    summary, details = run_exclusion_scenarios(delay_df, load_df, list(args.exclude_candidates))
    summary_csv, detail_csv, md = write_outputs(summary, details, out_dir)

    print(f"[ablation] summary={summary_csv}")
    print(f"[ablation] detail={detail_csv}")
    print(f"[ablation] markdown={md}")


if __name__ == "__main__":
    main()
