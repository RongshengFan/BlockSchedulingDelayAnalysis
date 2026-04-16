#!/usr/bin/env python3
"""Consistency guard checks for generated metric artifacts.

This module checks internal consistency across metric tables and ranking output.
It is read-only and does not modify existing pipeline data.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class GuardIssue:
    check: str
    severity: str
    detail: str

    def as_dict(self) -> dict[str, str]:
        return {"check": self.check, "severity": self.severity, "detail": self.detail}


def _read_csv(path: Path, required: list[str]) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"missing file: {path}")
    df = pd.read_csv(path)
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise SystemExit(f"missing columns in {path.name}: {miss}")
    return df


def load_tables(metrics_dir: Path, rank_file: Path | None = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sched_path = metrics_dir / "sched_summary_by_workload_batch.csv"
    if sched_path.exists():
        delay = _read_csv(
            sched_path,
            ["workload", "batch", "sched_cycles_per_sm_mean", "dispatch_gap_p95_cycles", "dispatch_gap_max_cycles"],
        )
    else:
        delay = _read_csv(
            metrics_dir / "delay_summary_by_workload_batch.csv",
            ["workload", "batch", "delay_mean", "delay_p95", "delay_p99"],
        )
    load = _read_csv(
        metrics_dir / "load_summary_by_workload_batch.csv",
        ["workload", "batch", "block_imbalance_ratio", "elapsed_sum_cv", "jain_block_fairness"],
    )
    rank_path = rank_file if rank_file is not None else metrics_dir / "workload_risk_ranking.csv"
    rank = _read_csv(
        rank_path,
        ["workload", "overall_risk_score", "rank_imbalance", "rank_cv", "rank_fairness_bad"],
    )
    if "rank_sched" not in rank.columns and "rank_delay" in rank.columns:
        rank["rank_sched"] = rank["rank_delay"]
    if "rank_delay" not in rank.columns and "rank_sched" in rank.columns:
        rank["rank_delay"] = rank["rank_sched"]
    return delay, load, rank


def _set_of_pairs(df: pd.DataFrame) -> set[tuple[str, int]]:
    return {(str(w), int(b)) for w, b in df[["workload", "batch"]].drop_duplicates().itertuples(index=False, name=None)}


def check_pair_coverage(delay: pd.DataFrame, load: pd.DataFrame) -> list[GuardIssue]:
    issues: list[GuardIssue] = []
    sd = _set_of_pairs(delay)
    sl = _set_of_pairs(load)
    only_delay = sorted(sd - sl)
    only_load = sorted(sl - sd)
    if only_delay:
        issues.append(GuardIssue("pair_coverage", "error", f"pairs only in delay table: {only_delay[:8]}"))
    if only_load:
        issues.append(GuardIssue("pair_coverage", "error", f"pairs only in load table: {only_load[:8]}"))
    return issues


def check_workload_coverage(delay: pd.DataFrame, load: pd.DataFrame, rank: pd.DataFrame) -> list[GuardIssue]:
    issues: list[GuardIssue] = []
    wd = set(delay["workload"].astype(str).unique().tolist())
    wl = set(load["workload"].astype(str).unique().tolist())
    wr = set(rank["workload"].astype(str).unique().tolist())
    if wd != wl:
        issues.append(GuardIssue("workload_coverage", "error", f"delay/load workloads differ: delay={sorted(wd)}, load={sorted(wl)}"))
    if wd != wr:
        issues.append(GuardIssue("workload_coverage", "warning", f"delay/ranking workloads differ: delay={sorted(wd)}, rank={sorted(wr)}"))
    return issues


def check_metric_ranges(delay: pd.DataFrame, load: pd.DataFrame) -> list[GuardIssue]:
    issues: list[GuardIssue] = []

    if "dispatch_gap_p95_cycles" in delay.columns:
        if (delay["sched_cycles_per_sm_mean"] < 0).any() or (delay["dispatch_gap_p95_cycles"] < 0).any() or (delay["dispatch_gap_max_cycles"] < 0).any():
            issues.append(GuardIssue("sched_ranges", "error", "sched metrics contain negative values"))
    else:
        if (delay["delay_mean"] < 0).any() or (delay["delay_p95"] < 0).any() or (delay["delay_p99"] < 0).any():
            issues.append(GuardIssue("delay_ranges", "error", "delay metrics contain negative values"))

    if not ((load["jain_block_fairness"] >= 0) & (load["jain_block_fairness"] <= 1)).all():
        issues.append(GuardIssue("jain_range", "error", "jain_block_fairness out of [0,1]"))

    if (load["block_imbalance_ratio"] < 0).any():
        issues.append(GuardIssue("imbalance_range", "error", "block_imbalance_ratio contains negative values"))

    if (load["elapsed_sum_cv"] < 0).any():
        issues.append(GuardIssue("elapsed_cv_range", "error", "elapsed_sum_cv contains negative values"))

    return issues


def check_rank_consistency(rank: pd.DataFrame) -> list[GuardIssue]:
    issues: list[GuardIssue] = []
    if "rank_sched" not in rank.columns and "rank_delay" in rank.columns:
        rank = rank.copy()
        rank["rank_sched"] = rank["rank_delay"]
    required = ["rank_sched", "rank_imbalance", "rank_cv", "rank_fairness_bad"]
    calc = rank[required].sum(axis=1)
    if not (calc == rank["overall_risk_score"]).all():
        issues.append(GuardIssue("rank_consistency", "error", "overall_risk_score != sum of component ranks"))

    # Expect sorting by descending risk score.
    if not rank["overall_risk_score"].is_monotonic_decreasing:
        issues.append(GuardIssue("rank_order", "warning", "ranking table is not sorted by descending overall_risk_score"))

    # Component ranks should be positive.
    if (rank[required] <= 0).any().any():
        issues.append(GuardIssue("rank_positive", "error", "rank components contain non-positive values"))

    return issues


def run_guards(delay: pd.DataFrame, load: pd.DataFrame, rank: pd.DataFrame) -> list[GuardIssue]:
    issues: list[GuardIssue] = []
    issues.extend(check_pair_coverage(delay, load))
    issues.extend(check_workload_coverage(delay, load, rank))
    issues.extend(check_metric_ranges(delay, load))
    issues.extend(check_rank_consistency(rank))
    return issues


def summarize(issues: list[GuardIssue]) -> dict[str, Any]:
    by_sev = {
        "error": sum(1 for i in issues if i.severity == "error"),
        "warning": sum(1 for i in issues if i.severity == "warning"),
        "info": sum(1 for i in issues if i.severity == "info"),
    }
    return {
        "passed": by_sev["error"] == 0,
        "issue_counts": by_sev,
        "total_issues": len(issues),
    }


def write_report(issues: list[GuardIssue], out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "summary": summarize(issues),
        "issues": [i.as_dict() for i in issues],
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run consistency guard checks on generated metric artifacts")
    parser.add_argument("--metrics-dir", default="../output/chart/metrics/base", help="base metrics directory")
    parser.add_argument(
        "--rank-file",
        default="../output/chart/metrics/report/workload_risk_ranking.csv",
        help="ranking csv path",
    )
    parser.add_argument("--out", default="../output/chart/metrics/guard/metrics_guard_report.json", help="output report json path")
    args = parser.parse_args()

    metrics_dir = (BASE_DIR / args.metrics_dir).resolve() if not Path(args.metrics_dir).is_absolute() else Path(args.metrics_dir)
    rank_file = (BASE_DIR / args.rank_file).resolve() if not Path(args.rank_file).is_absolute() else Path(args.rank_file)
    out = (BASE_DIR / args.out).resolve() if not Path(args.out).is_absolute() else Path(args.out)

    delay, load, rank = load_tables(metrics_dir, rank_file=rank_file)
    issues = run_guards(delay, load, rank)
    out = write_report(issues, out)
    s = summarize(issues)

    print(f"[guard] passed={s['passed']} errors={s['issue_counts']['error']} warnings={s['issue_counts']['warning']}")
    print(f"[guard] report={out}")

    if not s["passed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
