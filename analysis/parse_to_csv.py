#!/usr/bin/env python3
"""Parse blocked_sched traces into per-workload CSV tables.

Input layout:
  traces/<workload>/bs<batch>/trace/<trace_id>/result/*.bin

Output layout:
    output/data/<workload>.csv

Columns are normalized to thesis-safe names:
    workload,batch,block_id,sm,launch_anchor_ts,start_ts,launch_offset,elapsed,sched
"""

from __future__ import annotations

import argparse
import glob
import importlib.util
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent


def infer_tags(bin_path: str) -> tuple[str, int | None, str, str]:
    parts = bin_path.replace("\\", "/").split("/")
    workload = ""
    batch = None
    trace_id = ""
    kernel_run_id = Path(bin_path).stem

    for i, part in enumerate(parts):
        if part.startswith("bs") and part[2:].isdigit() and i >= 1:
            workload = parts[i - 1]
            batch = int(part[2:])
            if i + 2 < len(parts) and parts[i + 1] == "trace":
                trace_id = parts[i + 2]
            break

    return workload, batch, trace_id, kernel_run_id


def load_parse(bin_file: str):
    trace_dir = Path(bin_file).parent.parent
    read_py = trace_dir / "read.py"
    if not read_py.exists():
        raise FileNotFoundError(f"read.py not found for {bin_file}")

    module_name = f"trace_read_{abs(hash(str(read_py)))}"
    spec = importlib.util.spec_from_file_location(module_name, str(read_py))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed load parser: {read_py}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.parse


def parse_one_bin_block_level(bin_file: str, require_start_time: bool = True) -> list[dict]:
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

        if hasattr(rec, "start_time"):
            start = int(rec.start_time)
            start_clock = int(getattr(rec, "start_clock"))
            elapsed = int(rec.elapsed)
            sm = int(rec.sm)
        else:
            if require_start_time:
                raise RuntimeError(
                    f"{bin_file} does not contain start_time/globaltimer, "
                    "please collect with probe/probe.py"
                )
            start = int(rec.start)
            start_clock = int(rec.start)
            elapsed = int(rec.elapsed)
            sm = int(rec.cuid)

        rows.append(
            {
                "workload": workload,
                "batch": batch,
                "block_id": block_id,
                "sm": sm,
                "start_clock": start_clock,
                "start_ts": start,
                "elapsed": elapsed,
                "_trace_id": trace_id,
                "_kernel_run_id": kernel_run_id,
            }
        )
    return rows


def attach_sched_per_block(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["end_clock"] = out["start_clock"] + out["elapsed"]
    out["sched"] = 0

    group_cols = ["workload", "batch", "_trace_id", "_kernel_run_id"]
    for _, run_df in out.groupby(group_cols, sort=False):
        for _, sm_df in run_df.groupby("sm", sort=False):
            ordered = sm_df.sort_values(["start_clock", "end_clock", "block_id"])
            slots: list[dict] = []
            for row in ordered.itertuples():
                ended_slots = [slot for slot in slots if slot["end_clock"] <= row.start_clock]
                if ended_slots:
                    replaced = max(ended_slots, key=lambda slot: slot["end_clock"])
                    out.at[row.Index, "sched"] = int(row.start_clock - replaced["end_clock"])
                    slots.remove(replaced)
                slots.append({"index": row.Index, "end_clock": int(row.end_clock)})

    return out


def collect_bins(patterns: list[str]) -> list[str]:
    files = []
    for p in patterns:
        files.extend(glob.glob(p, recursive=True))
    return sorted({f for f in files if f.endswith(".bin")})


def build_block_table(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["workload", "batch", "_trace_id", "_kernel_run_id"]
    out = attach_sched_per_block(df)
    out["launch_anchor_ts"] = out.groupby(group_cols)["start_ts"].transform("min")
    out["launch_offset"] = out["start_ts"] - out["launch_anchor_ts"]

    out = out[
        [
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
    ]
    return out.sort_values(["workload", "batch", "block_id", "start_ts"]).reset_index(drop=True)


def write_outputs(df: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    # Remove stale workload CSVs so output/data always reflects current selection.
    for old_csv in out_dir.glob("*.csv"):
        old_csv.unlink()

    for workload in sorted(df["workload"].dropna().unique()):
        sub = df[df["workload"] == workload]
        sub.to_csv(out_dir / f"{workload}.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse blocked_sched traces to per-workload CSV")
    parser.add_argument("paths", nargs="*", help="glob patterns to .bin files")
    parser.add_argument("--output-dir", default="../output/data", help="output data directory")
    parser.add_argument(
        "--exclude-workloads",
        nargs="*",
        default=[],
        help="workload names to exclude from exported CSVs",
    )
    parser.add_argument(
        "--allow-legacy-no-start-time",
        action="store_true",
        help="allow old traces without start_time/globaltimer",
    )
    args = parser.parse_args()

    patterns = args.paths if args.paths else [str((BASE_DIR / "../traces/*/bs*/trace/*/result/*.bin").resolve())]
    bins = collect_bins(patterns)
    if not bins:
        raise SystemExit("no .bin files found")

    rows = []
    for bin_file in bins:
        rows.extend(parse_one_bin_block_level(bin_file, require_start_time=not args.allow_legacy_no_start_time))

    if not rows:
        raise SystemExit("no records parsed")

    block_df = pd.DataFrame(rows)
    block_df = build_block_table(block_df)
    exclude = {w.strip() for w in args.exclude_workloads if w and w.strip()}
    if exclude:
        block_df = block_df[~block_df["workload"].isin(exclude)].reset_index(drop=True)

    if block_df.empty:
        raise SystemExit("all rows filtered out after workload exclusion")

    out_dir = (BASE_DIR / args.output_dir).resolve() if not Path(args.output_dir).is_absolute() else Path(args.output_dir)
    write_outputs(block_df, out_dir)

    print(f"[done] rows={len(block_df)}")
    print(f"[done] per-workload csv at {out_dir}")


if __name__ == "__main__":
    main()
