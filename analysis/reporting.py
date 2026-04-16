#!/usr/bin/env python3
"""Generate structured analysis reports from computed metrics.

This module reads existing metric CSVs and validation outputs, then produces
JSON and Markdown summaries suitable for thesis writing.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_EXCLUDED_BATCHES = {8}


@dataclass(frozen=True)
class Finding:
    category: str
    workload: str
    batch: int | None
    severity: str
    metric: str
    value: float
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "workload": self.workload,
            "batch": self.batch,
            "severity": self.severity,
            "metric": self.metric,
            "value": self.value,
            "detail": self.detail,
        }


def _read_csv(path: Path, required: list[str]) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"missing file: {path}")
    df = pd.read_csv(path)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"missing columns in {path.name}: {missing}")
    return df


def _read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _parse_excluded_batches(value: str | None) -> set[int]:
    if value is None:
        return set(DEFAULT_EXCLUDED_BATCHES)
    raw = value.strip()
    if raw == "":
        return set()
    if raw.lower() in {"none", "null", "no"}:
        return set()
    out: set[int] = set()
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        out.add(int(item))
    return out


def _filter_df_by_batches(df: pd.DataFrame, excluded_batches: set[int]) -> pd.DataFrame:
    if df.empty or not excluded_batches or "batch" not in df.columns:
        return df
    batches = pd.to_numeric(df["batch"], errors="coerce")
    return df[~batches.isin(list(excluded_batches))].copy()


def _filter_validation_batches(val: dict[str, Any], excluded_batches: set[int]) -> dict[str, Any]:
    if not val or not excluded_batches:
        return val
    batches = val.get("batches")
    if not isinstance(batches, list):
        return val
    filtered: list[int] = []
    for item in batches:
        try:
            b = int(item)
        except (TypeError, ValueError):
            continue
        if b in excluded_batches:
            continue
        filtered.append(b)
    out = dict(val)
    out["batches"] = filtered
    return out


def load_metrics(metrics_dir: Path, validation_summary: Path | None = None) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    sched_path = metrics_dir / "sched_summary_by_workload_batch.csv"
    delay_path = metrics_dir / "delay_summary_by_workload_batch.csv"
    if sched_path.exists():
        delay = _read_csv(
            sched_path,
            [
                "workload",
                "batch",
                "sched_cycles_per_sm_mean",
                "dispatch_gap_p95_cycles",
                "dispatch_gap_max_cycles",
                "dispatch_gap_mean_cycles",
                "dispatch_gap_event_count",
            ],
        )
    else:
        delay = _read_csv(
            delay_path,
            ["workload", "batch", "delay_mean", "delay_p95", "delay_p99", "gap_mean", "gap_p95", "records"],
        )
    load = _read_csv(
        metrics_dir / "load_summary_by_workload_batch.csv",
        [
            "workload",
            "batch",
            "sm_count",
            "block_imbalance_ratio",
            "elapsed_sum_cv",
            "jain_block_fairness",
            "block_mean",
        ],
    )
    val_path = validation_summary if validation_summary is not None else metrics_dir / "validation_summary.json"
    if not val_path.exists() and validation_summary is None:
        # Compatibility fallback for split metrics layout.
        alt = metrics_dir.parent / "validation" / "validation_summary.json"
        if alt.exists():
            val_path = alt

    val = _read_json(
        val_path,
        {
            "rows": 0,
            "workloads": [],
            "batches": [],
            "issue_counts": {"error": 0, "warning": 0, "info": 0},
            "total_issues": 0,
            "passed": False,
        },
    )
    return delay, load, val


def _core_metric_spec(delay_df: pd.DataFrame) -> dict[str, str]:
    if "dispatch_gap_p95_cycles" in delay_df.columns:
        return {
            "category": "sched",
            "p95": "dispatch_gap_p95_cycles",
            "mean": "dispatch_gap_mean_cycles",
            "tail": "dispatch_gap_max_cycles",
            "records": "dispatch_gap_event_count",
            "agg_mean": "core_sched_p95_mean",
            "detail_name": "dispatch-gap p95 scheduling delay",
            "detail_short": "dispatch gap p95",
        }
    return {
        "category": "launch_offset",
        "p95": "delay_p95",
        "mean": "delay_mean",
        "tail": "delay_p99",
        "records": "records",
        "agg_mean": "core_sched_p95_mean",
        "detail_name": "relative launch-offset p95",
        "detail_short": "launch offset p95",
    }


def build_metric_definitions(delay_df: pd.DataFrame) -> dict[str, str]:
    spec = _core_metric_spec(delay_df)
    core_metric_desc = (
        "Core scheduling summary is based on Neutrino-style dispatch-gap statistics. "
        "For blocks observed on the same SM, the sched gap is defined as the actual "
        "start time of the next block minus the actual end time of the previous block."
        if spec["category"] == "sched"
        else "Core scheduling summary falls back to relative launch-offset statistics "
        "when Neutrino-style dispatch-gap summaries are unavailable."
    )
    return {
        "sched": (
            "sched denotes the Neutrino-style scheduling gap on the same SM: the actual "
            "start time of a newly dispatched block minus the actual end time of the "
            "previous block observed on that SM."
        ),
        "launch_offset": (
            "launch_offset is a relative start offset measured from the earliest block "
            "start timestamp in the same traced kernel run. It is not the true ready-to-run wait time."
        ),
        "core_sched_metric": core_metric_desc,
    }


def build_ranking_method_summary(
    delay_df: pd.DataFrame,
    excluded_batches: list[int] | None = None,
) -> dict[str, Any]:
    spec = _core_metric_spec(delay_df)
    return {
        "method": "equal_weight_rank_sum",
        "aggregation_level": "per-workload mean over retained batches",
        "core_sched_metric": spec["p95"],
        "dimensions": [
            {"name": "sched", "column": "core_sched_p95_mean", "direction": "higher_worse"},
            {"name": "imbalance", "column": "imbalance_mean", "direction": "higher_worse"},
            {"name": "elapsed_cv", "column": "elapsed_cv_mean", "direction": "higher_worse"},
            {"name": "fairness", "column": "jain_mean", "direction": "lower_worse"},
        ],
        "score_definition": "overall_risk_score = rank_sched + rank_imbalance + rank_cv + rank_fairness_bad",
        "explanation": (
            "A rank-sum is used instead of directly combining raw values, because the four "
            "dimensions have different units and scales. Each dimension is equally weighted."
        ),
        "excluded_batches": list(excluded_batches or []),
        "note": "This score is a comparative workload summary for analysis/dashboard use, not a physical hardware metric.",
    }


def _corr_strength(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "insufficient"
    abs_value = abs(float(value))
    if abs_value >= 0.8:
        return "very strong"
    if abs_value >= 0.6:
        return "strong"
    if abs_value >= 0.4:
        return "moderate"
    if abs_value >= 0.2:
        return "weak"
    return "very weak"


def _corr_direction(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "undetermined"
    return "positive" if float(value) >= 0 else "negative"


def _pairwise_correlation(df: pd.DataFrame, x: str, y: str, label: str) -> dict[str, Any]:
    pair = df[[x, y]].apply(pd.to_numeric, errors="coerce").dropna()
    if len(pair) < 2:
        return {
            "label": label,
            "x": x,
            "y": y,
            "count": int(len(pair)),
            "pearson": None,
            "spearman": None,
            "interpretation": "insufficient data",
        }

    pearson = pair[x].corr(pair[y], method="pearson")
    rank_x = pair[x].rank(method="average")
    rank_y = pair[y].rank(method="average")
    spearman = rank_x.corr(rank_y, method="pearson")
    ref = spearman if pd.notna(spearman) else pearson
    return {
        "label": label,
        "x": x,
        "y": y,
        "count": int(len(pair)),
        "pearson": float(pearson) if pd.notna(pearson) else None,
        "spearman": float(spearman) if pd.notna(spearman) else None,
        "interpretation": f"{_corr_strength(ref)} {_corr_direction(ref)} correlation",
    }


def build_correlation_summary(delay_df: pd.DataFrame, load_df: pd.DataFrame) -> dict[str, Any]:
    spec = _core_metric_spec(delay_df)
    if delay_df.empty or load_df.empty:
        return {"core_metric": spec["p95"], "merged_rows": 0, "pairwise": []}

    delay_cols = ["workload", "batch", spec["p95"]]
    for optional in ["dispatch_gap_event_count", "block_count_total"]:
        if optional in delay_df.columns:
            delay_cols.append(optional)

    delay_view = delay_df[delay_cols].copy()
    load_view = load_df[
        ["workload", "batch", "block_imbalance_ratio", "elapsed_sum_cv", "jain_block_fairness"]
    ].copy()
    merged = delay_view.merge(load_view, on=["workload", "batch"], how="inner")
    if merged.empty:
        return {"core_metric": spec["p95"], "merged_rows": 0, "pairwise": []}

    pairwise = [
        _pairwise_correlation(
            merged,
            spec["p95"],
            "block_imbalance_ratio",
            f"{spec['p95']} vs block_imbalance_ratio",
        ),
        _pairwise_correlation(
            merged,
            spec["p95"],
            "elapsed_sum_cv",
            f"{spec['p95']} vs elapsed_sum_cv",
        ),
        _pairwise_correlation(
            merged,
            spec["p95"],
            "jain_block_fairness",
            f"{spec['p95']} vs jain_block_fairness",
        ),
    ]

    if "dispatch_gap_event_count" in merged.columns and "block_count_total" in merged.columns:
        denom = pd.to_numeric(merged["block_count_total"], errors="coerce").replace(0, pd.NA)
        merged["sched_event_ratio"] = pd.to_numeric(merged["dispatch_gap_event_count"], errors="coerce") / denom
        pairwise.extend(
            [
                _pairwise_correlation(
                    merged,
                    "sched_event_ratio",
                    "block_imbalance_ratio",
                    "sched_event_ratio vs block_imbalance_ratio",
                ),
                _pairwise_correlation(
                    merged,
                    "sched_event_ratio",
                    "elapsed_sum_cv",
                    "sched_event_ratio vs elapsed_sum_cv",
                ),
                _pairwise_correlation(
                    merged,
                    "sched_event_ratio",
                    "jain_block_fairness",
                    "sched_event_ratio vs jain_block_fairness",
                ),
            ]
        )

    return {
        "core_metric": spec["p95"],
        "merged_rows": int(len(merged)),
        "pairwise": pairwise,
    }


def build_sched_findings(delay_df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    if delay_df.empty:
        return findings

    spec = _core_metric_spec(delay_df)
    p95_col = spec["p95"]
    mean_col = spec["mean"]
    tail_col = spec["tail"]
    records_col = spec["records"]

    for batch, sub in delay_df.groupby("batch"):
        worst = sub.sort_values(p95_col, ascending=False).iloc[0]
        findings.append(
            Finding(
                category=spec["category"],
                workload=str(worst["workload"]),
                batch=int(batch),
                severity="high",
                metric=p95_col,
                value=float(worst[p95_col]),
                detail=(
                    f"batch={int(batch)} highest {spec['detail_short']} workload is {worst['workload']} "
                    f"(p95={float(worst[p95_col]):.4f})"
                ),
            )
        )

    by_work = delay_df.groupby("workload", as_index=False).agg(
        core_sched_p95_mean=(p95_col, "mean"),
        core_sched_tail_mean=(tail_col, "mean"),
        core_sched_mean=(mean_col, "mean"),
        records_total=(records_col, "sum"),
    )

    slow = by_work.sort_values("core_sched_p95_mean", ascending=False).iloc[0]
    fast = by_work.sort_values("core_sched_p95_mean", ascending=True).iloc[0]
    findings.append(
        Finding(
            category=spec["category"],
            workload=str(slow["workload"]),
            batch=None,
            severity="high",
            metric="core_sched_p95_mean",
            value=float(slow["core_sched_p95_mean"]),
            detail=(
                f"overall highest mean {spec['detail_name']} workload is {slow['workload']} "
                f"(value={float(slow['core_sched_p95_mean']):.4f})"
            ),
        )
    )
    findings.append(
        Finding(
            category=spec["category"],
            workload=str(fast["workload"]),
            batch=None,
            severity="info",
            metric="core_sched_p95_mean",
            value=float(fast["core_sched_p95_mean"]),
            detail=(
                f"overall lowest mean {spec['detail_name']} workload is {fast['workload']} "
                f"(value={float(fast['core_sched_p95_mean']):.4f})"
            ),
        )
    )

    return findings


def build_delay_findings(delay_df: pd.DataFrame) -> list[Finding]:
    return build_sched_findings(delay_df)


def build_load_findings(load_df: pd.DataFrame) -> list[Finding]:
    findings: list[Finding] = []
    if load_df.empty:
        return findings

    by_work = load_df.groupby("workload", as_index=False).agg(
        imb_mean=("block_imbalance_ratio", "mean"),
        cv_mean=("elapsed_sum_cv", "mean"),
        jain_mean=("jain_block_fairness", "mean"),
        sm_count_mean=("sm_count", "mean"),
        block_mean=("block_mean", "mean"),
    )

    worst_imb = by_work.sort_values("imb_mean", ascending=False).iloc[0]
    best_imb = by_work.sort_values("imb_mean", ascending=True).iloc[0]
    worst_fair = by_work.sort_values("jain_mean", ascending=True).iloc[0]

    findings.extend(
        [
            Finding(
                category="load",
                workload=str(worst_imb["workload"]),
                batch=None,
                severity="high",
                metric="block_imbalance_ratio_mean",
                value=float(worst_imb["imb_mean"]),
                detail=(
                    f"highest average block imbalance ratio workload is {worst_imb['workload']} "
                    f"(value={float(worst_imb['imb_mean']):.4f})"
                ),
            ),
            Finding(
                category="load",
                workload=str(best_imb["workload"]),
                batch=None,
                severity="info",
                metric="block_imbalance_ratio_mean",
                value=float(best_imb["imb_mean"]),
                detail=(
                    f"lowest average block imbalance ratio workload is {best_imb['workload']} "
                    f"(value={float(best_imb['imb_mean']):.4f})"
                ),
            ),
            Finding(
                category="load",
                workload=str(worst_fair["workload"]),
                batch=None,
                severity="high",
                metric="jain_block_fairness_mean",
                value=float(worst_fair["jain_mean"]),
                detail=(
                    f"lowest average Jain fairness workload is {worst_fair['workload']} "
                    f"(value={float(worst_fair['jain_mean']):.4f})"
                ),
            ),
        ]
    )

    for batch, sub in load_df.groupby("batch"):
        sub_bad = sub.sort_values("block_imbalance_ratio", ascending=False).iloc[0]
        findings.append(
            Finding(
                category="load",
                workload=str(sub_bad["workload"]),
                batch=int(batch),
                severity="medium",
                metric="block_imbalance_ratio",
                value=float(sub_bad["block_imbalance_ratio"]),
                detail=(
                    f"batch={int(batch)} largest imbalance observed on {sub_bad['workload']} "
                    f"(ratio={float(sub_bad['block_imbalance_ratio']):.4f})"
                ),
            )
        )

    return findings


def aggregate_ranking(delay_df: pd.DataFrame, load_df: pd.DataFrame) -> pd.DataFrame:
    spec = _core_metric_spec(delay_df)
    delay_rank = (
        delay_df.groupby("workload", as_index=False)[spec["p95"]]
        .mean()
        .rename(columns={spec["p95"]: "core_sched_p95_mean"})
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
    rank["rank_sched"] = rank["core_sched_p95_mean"].rank(ascending=True, method="min")
    rank["rank_imbalance"] = rank["imbalance_mean"].rank(ascending=True, method="min")
    rank["rank_cv"] = rank["elapsed_cv_mean"].rank(ascending=True, method="min")
    rank["rank_fairness_bad"] = rank["jain_mean"].rank(ascending=False, method="min")
    rank["overall_risk_score"] = rank[["rank_sched", "rank_imbalance", "rank_cv", "rank_fairness_bad"]].sum(axis=1)
    rank["delay_p95_mean"] = rank["core_sched_p95_mean"]
    rank["rank_delay"] = rank["rank_sched"]
    rank = rank.sort_values("overall_risk_score", ascending=False).reset_index(drop=True)
    return rank


def build_conclusion(
    delay_df: pd.DataFrame,
    load_df: pd.DataFrame,
    val: dict[str, Any],
    rank_df: pd.DataFrame,
    excluded_batches: list[int] | None = None,
) -> dict[str, Any]:
    sched_findings = build_sched_findings(delay_df)
    load_findings = build_load_findings(load_df)
    corr_summary = build_correlation_summary(delay_df, load_df)

    top_risk = rank_df.iloc[0].to_dict() if not rank_df.empty else {}
    low_risk = rank_df.iloc[-1].to_dict() if not rank_df.empty else {}

    return {
        "validation": val,
        "metric_definitions": build_metric_definitions(delay_df),
        "ranking_method": build_ranking_method_summary(delay_df, excluded_batches=excluded_batches),
        "correlation_summary": corr_summary,
        "sched_findings": [x.as_dict() for x in sched_findings],
        "delay_findings": [x.as_dict() for x in sched_findings],
        "load_findings": [x.as_dict() for x in load_findings],
        "top_risk_workload": top_risk,
        "lowest_risk_workload": low_risk,
        "n_workloads": int(rank_df["workload"].nunique()) if "workload" in rank_df.columns else 0,
    }


def to_markdown(conclusion: dict[str, Any], rank_df: pd.DataFrame) -> str:
    val = conclusion["validation"]
    sched_findings = conclusion.get("sched_findings", conclusion.get("delay_findings", []))
    load_findings = conclusion["load_findings"]
    metric_defs = conclusion.get("metric_definitions", {})
    ranking_method = conclusion.get("ranking_method", {})
    corr_summary = conclusion.get("correlation_summary", {})

    lines: list[str] = []
    lines.append("# Scheduling Delay and Load Analysis Report")
    lines.append("")
    lines.append("## 1) Metric Notes")
    if metric_defs:
        lines.append(f"- sched: {metric_defs.get('sched', '')}")
        lines.append(f"- launch_offset: {metric_defs.get('launch_offset', '')}")
        lines.append(f"- core metric: {metric_defs.get('core_sched_metric', '')}")
    else:
        lines.append("- no metric notes")
    lines.append("")

    lines.append("## 2) Validation")
    lines.append(f"- rows: {val.get('rows', 0)}")
    lines.append(f"- passed: {val.get('passed', False)}")
    issue_counts = val.get("issue_counts", {})
    lines.append(f"- errors: {issue_counts.get('error', 0)}")
    lines.append(f"- warnings: {issue_counts.get('warning', 0)}")
    lines.append("")

    lines.append("## 3) Scheduling Findings")
    if sched_findings:
        for f in sched_findings:
            lines.append(f"- [{f['severity']}] {f['detail']}")
    else:
        lines.append("- no scheduling findings")
    lines.append("")

    lines.append("## 4) Load Findings")
    if load_findings:
        for f in load_findings:
            lines.append(f"- [{f['severity']}] {f['detail']}")
    else:
        lines.append("- no load findings")
    lines.append("")

    lines.append("## 5) Correlation Summary")
    if corr_summary.get("pairwise"):
        lines.append(f"- merged workload-batch rows: {corr_summary.get('merged_rows', 0)}")
        for item in corr_summary["pairwise"]:
            pearson = item.get("pearson")
            spearman = item.get("spearman")
            lines.append(
                "- "
                f"{item['label']}: pearson={pearson:.4f} "
                if pearson is not None
                else f"- {item['label']}: pearson=NA "
            )
            lines[-1] += (
                f"spearman={spearman:.4f}, {item.get('interpretation', '')}"
                if spearman is not None
                else f"spearman=NA, {item.get('interpretation', '')}"
            )
    else:
        lines.append("- no correlation summary")
    lines.append("")

    lines.append("## 6) Workload Composite Ranking")
    if ranking_method:
        lines.append(f"- method: {ranking_method.get('method', '-')}")
        lines.append(f"- score definition: {ranking_method.get('score_definition', '-')}")
        lines.append(f"- explanation: {ranking_method.get('explanation', '-')}")
        excluded = ranking_method.get("excluded_batches", [])
        if excluded:
            lines.append(f"- excluded batches: {', '.join(str(x) for x in excluded)}")
        lines.append("")
    if rank_df.empty:
        lines.append("- no ranking data")
    else:
        for _, row in rank_df.iterrows():
            lines.append(
                "- "
                f"{row['workload']}: score={float(row['overall_risk_score']):.2f}, "
                f"core_sched_p95_mean={float(row['core_sched_p95_mean']):.4f}, "
                f"imbalance_mean={float(row['imbalance_mean']):.4f}, "
                f"jain_mean={float(row['jain_mean']):.4f}"
            )
    lines.append("")

    top_risk = conclusion.get("top_risk_workload", {})
    low_risk = conclusion.get("lowest_risk_workload", {})
    lines.append("## 7) Conclusion")
    if top_risk and low_risk:
        lines.append(
            "- Highest scheduling risk workload: "
            f"{top_risk.get('workload', 'unknown')} "
            f"(score={float(top_risk.get('overall_risk_score', 0)):.2f})"
        )
        lines.append(
            "- Lowest scheduling risk workload: "
            f"{low_risk.get('workload', 'unknown')} "
            f"(score={float(low_risk.get('overall_risk_score', 0)):.2f})"
        )
    else:
        lines.append("- insufficient data to conclude workload risk ranking")

    return "\n".join(lines) + "\n"


def save_report(conclusion: dict[str, Any], rank_df: pd.DataFrame, out_dir: Path) -> tuple[Path, Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "analysis_conclusion.json"
    md_path = out_dir / "analysis_conclusion.md"
    rank_path = out_dir / "workload_risk_ranking.csv"

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(conclusion, f, ensure_ascii=False, indent=2)
    with md_path.open("w", encoding="utf-8") as f:
        f.write(to_markdown(conclusion, rank_df))
    rank_df.to_csv(rank_path, index=False)

    return json_path, md_path, rank_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build structured analysis conclusion from metrics")
    parser.add_argument("--metrics-dir", default="../output/chart/metrics/base", help="directory containing base metric CSVs")
    parser.add_argument(
        "--validation-summary",
        default="../output/chart/metrics/validation/validation_summary.json",
        help="validation summary json path",
    )
    parser.add_argument("--out-dir", default="../output/chart/metrics/report", help="directory to write report files")
    parser.add_argument("--exclude-batches", default="8", help="comma-separated batches to ignore (use 'none' to disable)")
    args = parser.parse_args()

    metrics_dir = (BASE_DIR / args.metrics_dir).resolve() if not Path(args.metrics_dir).is_absolute() else Path(args.metrics_dir)
    val_path = (
        (BASE_DIR / args.validation_summary).resolve()
        if not Path(args.validation_summary).is_absolute()
        else Path(args.validation_summary)
    )
    out_dir = (BASE_DIR / args.out_dir).resolve() if not Path(args.out_dir).is_absolute() else Path(args.out_dir)

    delay_df, load_df, val = load_metrics(metrics_dir, validation_summary=val_path)
    excluded_batches = _parse_excluded_batches(args.exclude_batches)
    delay_df = _filter_df_by_batches(delay_df, excluded_batches)
    load_df = _filter_df_by_batches(load_df, excluded_batches)
    val = _filter_validation_batches(val, excluded_batches)
    rank_df = aggregate_ranking(delay_df, load_df)
    conclusion = build_conclusion(delay_df, load_df, val, rank_df, excluded_batches=sorted(excluded_batches))
    json_path, md_path, rank_path = save_report(conclusion, rank_df, out_dir)

    print(f"[report] metrics_dir={metrics_dir}")
    print(f"[report] out_json={json_path}")
    print(f"[report] out_md={md_path}")
    print(f"[report] out_rank_csv={rank_path}")


if __name__ == "__main__":
    main()
