#!/usr/bin/env python3
"""Recompute Neutrino-style scheduling metrics from raw block traces.

This script does not replace the existing delay metric. Instead, it derives a
second metric family directly from raw traces using the same scheduling-gap
approximation described by Neutrino's block_sched example: for blocks observed
on the same SM, if a new block starts after a previously recorded block on that
SM has ended, the gap is treated as a block replacement scheduling interval.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from parse_to_csv import collect_bins, infer_tags, load_parse


def parse_one_bin_sched_rows(bin_file: str) -> list[dict]:
    parse = load_parse(bin_file)
    _, _, records = parse(bin_file)
    key = next(iter(records.keys()))
    data = records[key]

    workload, batch, trace_id, kernel_run_id = infer_tags(bin_file)

    rows: list[dict] = []
    for block_id, block in enumerate(data):
        if not block or not block[0] or not block[0][0]:
            continue
        rec = block[0][0]
        start_clock = getattr(rec, "start_clock", getattr(rec, "start", None))
        if start_clock is None:
            raise RuntimeError(f"{bin_file} does not contain start_clock/clock64")
        start_time = getattr(rec, "start_time", None)
        sm = getattr(rec, "sm", getattr(rec, "cuid", None))
        if sm is None:
            raise RuntimeError(f"{bin_file} does not contain sm/cuid")

        start_clock = int(start_clock)
        elapsed = int(rec.elapsed)
        start_time_val = int(start_time) if start_time is not None else None
        sm = int(sm)

        rows.append(
            {
                "workload": workload,
                "batch": batch,
                "trace_id": trace_id,
                "kernel_run_id": kernel_run_id,
                "block_id": block_id,
                "sm": sm,
                "start_clock": start_clock,
                "start_time": start_time_val,
                "elapsed": elapsed,
                "end_clock": start_clock + elapsed,
            }
        )
    return rows


def simulate_sm_dispatch_gaps(run_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    detail_rows: list[dict] = []
    event_rows: list[dict] = []

    id_cols = ["workload", "batch", "trace_id", "kernel_run_id"]
    run_meta = {col: run_df.iloc[0][col] for col in id_cols}

    for sm, sm_df in run_df.groupby("sm", sort=True):
        blocks = sm_df.sort_values(["start_clock", "end_clock", "block_id"]).to_dict("records")
        slots: list[dict] = []
        sched_gaps: list[int] = []

        for block in blocks:
            ended_slots = [slot for slot in slots if slot["end_clock"] <= block["start_clock"]]
            if ended_slots:
                replaced = max(ended_slots, key=lambda slot: slot["end_clock"])
                gap = int(block["start_clock"] - replaced["end_clock"])
                sched_gaps.append(gap)
                event_rows.append(
                    {
                        **run_meta,
                        "sm": int(sm),
                        "prev_block_id": int(replaced["block_id"]),
                        "block_id": int(block["block_id"]),
                        "prev_end_clock": int(replaced["end_clock"]),
                        "start_clock": int(block["start_clock"]),
                        "dispatch_gap": gap,
                    }
                )
                slots.remove(replaced)

            slots.append(block)

        gap_series = pd.Series(sched_gaps, dtype="int64")
        elapsed_series = sm_df["elapsed"].astype("int64")
        detail_rows.append(
            {
                **run_meta,
                "sm": int(sm),
                "block_count": int(len(sm_df)),
                "dispatch_gap_event_count": int(len(sched_gaps)),
                "sched_cycles_total": int(gap_series.sum()) if not gap_series.empty else 0,
                "dispatch_gap_mean_cycles": float(gap_series.mean()) if not gap_series.empty else 0.0,
                "dispatch_gap_p95_cycles": float(gap_series.quantile(0.95)) if not gap_series.empty else 0.0,
                "dispatch_gap_max_cycles": int(gap_series.max()) if not gap_series.empty else 0,
                "work_cycles_total": int(elapsed_series.sum()),
                "block_elapsed_mean_cycles": float(elapsed_series.mean()) if not elapsed_series.empty else 0.0,
                "block_elapsed_p95_cycles": float(elapsed_series.quantile(0.95)) if not elapsed_series.empty else 0.0,
                "inferred_slot_count": int(len(slots)),
            }
        )

    detail_df = pd.DataFrame(detail_rows)
    event_df = pd.DataFrame(event_rows)
    return detail_df, event_df


def build_sched_tables(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    detail_parts: list[pd.DataFrame] = []
    event_parts: list[pd.DataFrame] = []

    group_cols = ["workload", "batch", "trace_id", "kernel_run_id"]
    for _, run_df in df.groupby(group_cols, sort=True):
        detail_df, event_df = simulate_sm_dispatch_gaps(run_df)
        detail_parts.append(detail_df)
        if not event_df.empty:
            event_parts.append(event_df)

    detail = pd.concat(detail_parts, ignore_index=True) if detail_parts else pd.DataFrame()
    events = pd.concat(event_parts, ignore_index=True) if event_parts else pd.DataFrame(
        columns=[
            "workload",
            "batch",
            "trace_id",
            "kernel_run_id",
            "sm",
            "prev_block_id",
            "block_id",
            "prev_end_clock",
            "start_clock",
            "dispatch_gap",
        ]
    )

    summary_rows: list[dict] = []
    for (workload, batch), sub in detail.groupby(["workload", "batch"], sort=True):
        sub_events = events[(events["workload"] == workload) & (events["batch"] == batch)]
        event_gap = sub_events["dispatch_gap"].astype("int64") if not sub_events.empty else pd.Series(dtype="int64")
        summary_rows.append(
            {
                "workload": workload,
                "batch": int(batch),
                "trace_count": int(sub["trace_id"].nunique()),
                "kernel_run_count": int(sub[["trace_id", "kernel_run_id"]].drop_duplicates().shape[0]),
                "sm_observed": int(sub["sm"].nunique()),
                "block_count_total": int(sub["block_count"].sum()),
                "dispatch_gap_event_count": int(sub["dispatch_gap_event_count"].sum()),
                "sched_cycles_per_sm_mean": float(sub["sched_cycles_total"].mean()),
                "sched_cycles_per_sm_p95": float(sub["sched_cycles_total"].quantile(0.95)),
                "dispatch_gap_mean_cycles": float(event_gap.mean()) if not event_gap.empty else 0.0,
                "dispatch_gap_p95_cycles": float(event_gap.quantile(0.95)) if not event_gap.empty else 0.0,
                "dispatch_gap_max_cycles": int(event_gap.max()) if not event_gap.empty else 0,
                "work_cycles_per_sm_mean": float(sub["work_cycles_total"].mean()),
                "block_elapsed_mean_cycles": float(sub["block_elapsed_mean_cycles"].mean()),
                "block_elapsed_p95_cycles": float(sub["block_elapsed_p95_cycles"].quantile(0.95)),
                "inferred_slot_count_mean": float(sub["inferred_slot_count"].mean()),
                "inferred_slot_count_max": int(sub["inferred_slot_count"].max()),
            }
        )

    summary = pd.DataFrame(summary_rows)
    if not detail.empty:
        detail = detail.sort_values(["workload", "batch", "trace_id", "kernel_run_id", "sm"]).reset_index(drop=True)
    if not events.empty:
        events = events.sort_values(["workload", "batch", "trace_id", "kernel_run_id", "sm", "start_clock"]).reset_index(drop=True)
    if not summary.empty:
        summary = summary.sort_values(["workload", "batch"]).reset_index(drop=True)
    return detail, events, summary


def write_outputs(detail: pd.DataFrame, events: pd.DataFrame, summary: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    detail.to_csv(out_dir / "sched_detail_by_sm.csv", index=False)
    events.to_csv(out_dir / "dispatch_gap_events.csv", index=False)
    summary.to_csv(out_dir / "sched_summary_by_workload_batch.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Recompute Neutrino-style scheduling metrics from raw traces")
    parser.add_argument("paths", nargs="*", help="glob patterns to .bin files")
    parser.add_argument("--out-dir", default="../output/chart/metrics/base", help="output directory for scheduling metric csv files")
    parser.add_argument(
        "--exclude-workloads",
        nargs="*",
        default=[],
        help="workload names to exclude from outputs",
    )
    args = parser.parse_args()

    patterns = args.paths if args.paths else [str((BASE_DIR / "../traces/*/bs*/trace/*/result/*.bin").resolve())]
    bins = collect_bins(patterns)
    if not bins:
        raise SystemExit("no .bin files found")

    rows: list[dict] = []
    for bin_file in bins:
        rows.extend(parse_one_bin_sched_rows(bin_file))

    if not rows:
        raise SystemExit("no records parsed")

    df = pd.DataFrame(rows)
    exclude = {w.strip() for w in args.exclude_workloads if w and w.strip()}
    if exclude:
        df = df[~df["workload"].isin(exclude)].reset_index(drop=True)

    if df.empty:
        raise SystemExit("all rows filtered out after workload exclusion")

    detail, events, summary = build_sched_tables(df)
    out_dir = (BASE_DIR / args.out_dir).resolve() if not Path(args.out_dir).is_absolute() else Path(args.out_dir)
    write_outputs(detail, events, summary, out_dir)

    print(f"[done] block_rows={len(df)}")
    print(f"[done] sched_detail_rows={len(detail)}")
    print(f"[done] dispatch_gap_events={len(events)}")
    print(f"[done] summary_rows={len(summary)}")
    print(f"[done] outputs at {out_dir}")


if __name__ == "__main__":
    main()