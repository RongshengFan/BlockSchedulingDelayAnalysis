#!/usr/bin/env python3
"""Data quality validators for block-level workload CSV files.

This module is intentionally read-only for the existing pipeline: it does not
change collection, parsing, or plotting behavior. It only checks output quality
and emits machine-readable reports.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent

REQUIRED_COLUMNS = [
    "workload",
    "batch",
    "block_id",
    "sm",
    "launch_anchor_ts",
    "start_ts",
    "launch_offset",
    "elapsed",
    "sched",
]


@dataclass(frozen=True)
class ValidationIssue:
    check: str
    severity: str
    workload: str
    batch: int | None
    row_index: int | None
    detail: str

    def to_dict(self) -> dict:
        return {
            "check": self.check,
            "severity": self.severity,
            "workload": self.workload,
            "batch": self.batch,
            "row_index": self.row_index,
            "detail": self.detail,
        }


def load_per_workload_csv(data_dir: Path) -> pd.DataFrame:
    files = sorted(data_dir.glob("*.csv"))
    if not files:
        raise SystemExit(f"no csv files found in {data_dir}")

    dfs = [pd.read_csv(p) for p in files]
    out = pd.concat(dfs, ignore_index=True)
    return out


def check_required_columns(df: pd.DataFrame) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        issues.append(
            ValidationIssue(
                check="required_columns",
                severity="error",
                workload="*",
                batch=None,
                row_index=None,
                detail=f"missing columns: {missing}",
            )
        )
    return issues


def check_nulls(df: pd.DataFrame, columns: Iterable[str]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for col in columns:
        if col not in df.columns:
            continue
        bad = df[df[col].isna()]
        for idx in bad.index.tolist()[:2000]:
            row = df.loc[idx]
            issues.append(
                ValidationIssue(
                    check="null_values",
                    severity="error",
                    workload=str(row.get("workload", "")),
                    batch=_safe_int(row.get("batch")),
                    row_index=int(idx),
                    detail=f"column '{col}' is null",
                )
            )
        if len(bad) > 2000:
            issues.append(
                ValidationIssue(
                    check="null_values",
                    severity="warning",
                    workload="*",
                    batch=None,
                    row_index=None,
                    detail=f"column '{col}' has {len(bad)} null rows, truncated to first 2000",
                )
            )
    return issues


def check_delay_definition(df: pd.DataFrame) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    needed = {"start_ts", "launch_anchor_ts", "launch_offset"}
    if not needed.issubset(df.columns):
        return issues

    calc = (df["start_ts"] - df["launch_anchor_ts"]).astype("Int64")
    mismatch = df[calc != df["launch_offset"]]
    for idx in mismatch.index.tolist()[:2000]:
        row = mismatch.loc[idx]
        issues.append(
            ValidationIssue(
                check="launch_offset_definition",
                severity="error",
                workload=str(row.get("workload", "")),
                batch=_safe_int(row.get("batch")),
                row_index=int(idx),
                detail=(
                    "launch_offset mismatch: expected start_ts-launch_anchor_ts="
                    f"{int(row['start_ts']) - int(row['launch_anchor_ts'])}, got {int(row['launch_offset'])}"
                ),
            )
        )

    negatives = df[df["launch_offset"] < 0]
    for idx in negatives.index.tolist()[:2000]:
        row = negatives.loc[idx]
        issues.append(
            ValidationIssue(
                check="negative_launch_offset",
                severity="error",
                workload=str(row.get("workload", "")),
                batch=_safe_int(row.get("batch")),
                row_index=int(idx),
                detail=(
                    "launch_offset is negative: "
                    f"launch_anchor_ts={int(row['launch_anchor_ts'])}, start_ts={int(row['start_ts'])}, launch_offset={int(row['launch_offset'])}"
                ),
            )
        )
    return issues


def check_elapsed_positive(df: pd.DataFrame) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if "elapsed" not in df.columns:
        return issues
    bad = df[df["elapsed"] <= 0]
    for idx in bad.index.tolist()[:2000]:
        row = bad.loc[idx]
        issues.append(
            ValidationIssue(
                check="elapsed_positive",
                severity="error",
                workload=str(row.get("workload", "")),
                batch=_safe_int(row.get("batch")),
                row_index=int(idx),
                detail=f"elapsed must be > 0, got {int(row['elapsed'])}",
            )
        )
    return issues


def check_sched_non_negative(df: pd.DataFrame) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if "sched" not in df.columns:
        return issues
    bad = df[df["sched"] < 0]
    for idx in bad.index.tolist()[:2000]:
        row = bad.loc[idx]
        issues.append(
            ValidationIssue(
                check="negative_sched",
                severity="error",
                workload=str(row.get("workload", "")),
                batch=_safe_int(row.get("batch")),
                row_index=int(idx),
                detail=f"sched must be >= 0, got {int(row['sched'])}",
            )
        )
    return issues


def check_ready_not_after_start(df: pd.DataFrame) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    needed = {"workload", "batch", "launch_anchor_ts", "start_ts"}
    if not needed.issubset(df.columns):
        return issues

    bad = df[df["launch_anchor_ts"] > df["start_ts"]]
    for idx in bad.index.tolist()[:2000]:
        row = bad.loc[idx]
        issues.append(
            ValidationIssue(
                check="launch_anchor_after_start",
                severity="error",
                workload=str(row.get("workload", "")),
                batch=_safe_int(row.get("batch")),
                row_index=int(idx),
                detail=(
                    "launch_anchor_ts must be <= start_ts, got "
                    f"launch_anchor_ts={int(row['launch_anchor_ts'])}, start_ts={int(row['start_ts'])}"
                ),
            )
        )
    return issues


def check_duplicate_blocks(df: pd.DataFrame) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    needed = {"workload", "batch", "block_id", "sm", "launch_anchor_ts", "start_ts", "launch_offset", "elapsed", "sched"}
    if not needed.issubset(df.columns):
        return issues

    key_cols = ["workload", "batch", "block_id", "sm", "launch_anchor_ts", "start_ts", "launch_offset", "elapsed", "sched"]
    dup = df[df.duplicated(key_cols, keep=False)]
    if dup.empty:
        return issues

    for _, sub in dup.groupby(key_cols):
        first = sub.iloc[0]
        issues.append(
            ValidationIssue(
                check="duplicate_blocks",
                severity="warning",
                workload=str(first["workload"]),
                batch=_safe_int(first["batch"]),
                row_index=None,
                detail=(
                    "duplicated full row found: "
                    f"block_id={int(first['block_id'])}, sm={int(first['sm'])}, count={len(sub)}"
                ),
            )
        )
    return issues


def check_sm_coverage(df: pd.DataFrame, min_sms: int = 1) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    needed = {"workload", "batch", "sm"}
    if not needed.issubset(df.columns):
        return issues

    for (workload, batch), sub in df.groupby(["workload", "batch"]):
        n_sms = int(sub["sm"].nunique())
        if n_sms < min_sms:
            issues.append(
                ValidationIssue(
                    check="sm_coverage",
                    severity="warning",
                    workload=str(workload),
                    batch=_safe_int(batch),
                    row_index=None,
                    detail=f"unique SM count is {n_sms}, below threshold {min_sms}",
                )
            )
    return issues


def compute_summary(df: pd.DataFrame, issues: list[ValidationIssue]) -> dict:
    by_sev: dict[str, int] = {}
    for sev in ["error", "warning", "info"]:
        by_sev[sev] = sum(1 for x in issues if x.severity == sev)

    workloads = sorted(df["workload"].dropna().astype(str).unique().tolist()) if "workload" in df.columns else []
    batches = sorted(df["batch"].dropna().astype(int).unique().tolist()) if "batch" in df.columns else []
    rows = int(len(df))

    return {
        "rows": rows,
        "workloads": workloads,
        "batches": batches,
        "issue_counts": by_sev,
        "total_issues": len(issues),
        "passed": by_sev["error"] == 0,
    }


def validate_dataframe(df: pd.DataFrame, min_sms: int = 1) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    checks = [
        check_required_columns(df),
    ]
    issues.extend(_flatten(checks))

    # If schema itself is broken, avoid cascading noisy checks.
    if any(i.check == "required_columns" and i.severity == "error" for i in issues):
        return issues

    checks = [
        check_nulls(df, REQUIRED_COLUMNS),
        check_delay_definition(df),
        check_elapsed_positive(df),
        check_sched_non_negative(df),
        check_ready_not_after_start(df),
        check_duplicate_blocks(df),
        check_sm_coverage(df, min_sms=min_sms),
    ]
    issues.extend(_flatten(checks))
    return issues


def save_reports(df: pd.DataFrame, issues: list[ValidationIssue], out_dir: Path) -> tuple[Path, Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    issue_df = pd.DataFrame([x.to_dict() for x in issues])
    summary = compute_summary(df, issues)

    summary_json = out_dir / "validation_summary.json"
    issue_csv = out_dir / "validation_issues.csv"
    issue_md = out_dir / "validation_summary.md"

    with summary_json.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    issue_df.to_csv(issue_csv, index=False)

    lines = [
        "# Validation Summary",
        "",
        f"- rows: {summary['rows']}",
        f"- workloads: {', '.join(summary['workloads']) if summary['workloads'] else '(none)'}",
        f"- batches: {', '.join(str(x) for x in summary['batches']) if summary['batches'] else '(none)'}",
        f"- errors: {summary['issue_counts']['error']}",
        f"- warnings: {summary['issue_counts']['warning']}",
        f"- total issues: {summary['total_issues']}",
        f"- passed: {summary['passed']}",
        "",
    ]
    with issue_md.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return summary_json, issue_csv, issue_md


def _safe_int(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _flatten(items: Iterable[list[ValidationIssue]]) -> list[ValidationIssue]:
    out: list[ValidationIssue] = []
    for chunk in items:
        out.extend(chunk)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate block-level CSV data quality")
    parser.add_argument("--data-dir", default="../output/data", help="input directory containing per-workload csv")
    parser.add_argument("--out-dir", default="../output/chart/metrics/validation", help="output directory for validation reports")
    parser.add_argument("--min-sms", type=int, default=1, help="minimum unique SM count per workload-batch")
    args = parser.parse_args()

    data_dir = (BASE_DIR / args.data_dir).resolve() if not Path(args.data_dir).is_absolute() else Path(args.data_dir)
    out_dir = (BASE_DIR / args.out_dir).resolve() if not Path(args.out_dir).is_absolute() else Path(args.out_dir)

    df = load_per_workload_csv(data_dir)
    issues = validate_dataframe(df, min_sms=max(1, int(args.min_sms)))
    summary_json, issue_csv, issue_md = save_reports(df, issues, out_dir)
    summary = compute_summary(df, issues)

    print(f"[validate] rows={summary['rows']}")
    print(f"[validate] issues={summary['total_issues']} (errors={summary['issue_counts']['error']}, warnings={summary['issue_counts']['warning']})")
    print(f"[validate] passed={summary['passed']}")
    print(f"[validate] summary_json={summary_json}")
    print(f"[validate] issue_csv={issue_csv}")
    print(f"[validate] summary_md={issue_md}")

    if not summary["passed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
