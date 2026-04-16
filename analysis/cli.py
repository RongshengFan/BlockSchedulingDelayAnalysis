#!/usr/bin/env python3
"""Pipeline CLI wrapper for collection, parsing, analysis, validation and reporting.

This wrapper only orchestrates existing scripts/commands and does not change the
underlying workload, parser, or metric logic.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    from config_loader import apply_config_to_args, config_to_dict, load_pipeline_config
except ModuleNotFoundError:  # pragma: no cover
    from config_loader import apply_config_to_args, config_to_dict, load_pipeline_config

ROOT_DIR = BASE_DIR.parent


@dataclass(frozen=True)
class CommandSpec:
    name: str
    argv: list[str]
    cwd: Path
    env_overrides: dict[str, str]


def python_cmd(python: str, script: Path, args: Sequence[str] | None = None) -> list[str]:
    argv = [python, str(script)]
    if args:
        argv.extend(args)
    return argv


def metric_subdirs(chart_dir: str) -> dict[str, str]:
    root = f"{chart_dir}/metrics"
    return {
        "root": root,
        "base": f"{root}/base",
        "validation": f"{root}/validation",
        "report": f"{root}/report",
        "ablation": f"{root}/ablation",
        "guard": f"{root}/guard",
        "scenario": f"{root}/scenario",
    }


def migrate_legacy_metric_outputs(chart_dir: str) -> None:
    m = metric_subdirs(chart_dir)
    root = (BASE_DIR / m["root"]).resolve() if not Path(m["root"]).is_absolute() else Path(m["root"]) 
    if not root.exists():
        return

    mapping = {
        # base metrics
        "load_per_sm.csv": "base",
        "load_summary_by_workload_batch.csv": "base",
        # validation
        "validation_summary.json": "validation",
        "validation_issues.csv": "validation",
        "validation_summary.md": "validation",
        # report
        "analysis_conclusion.json": "report",
        "analysis_conclusion.md": "report",
        "workload_risk_ranking.csv": "report",
        # ablation
        "ablation_exclusion_summary.csv": "ablation",
        "ablation_exclusion_detail.csv": "ablation",
        "ablation_exclusion_summary.md": "ablation",
        # guard
        "metrics_guard_report.json": "guard",
        # scenario
        "scenario_matrix_summary.json": "scenario",
    }

    for name, group in mapping.items():
        src = root / name
        if not src.exists():
            continue
        dst_dir = (BASE_DIR / m[group]).resolve() if not Path(m[group]).is_absolute() else Path(m[group])
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / name
        src.replace(dst)


def collect_spec(args: argparse.Namespace) -> CommandSpec:
    env = {}
    if args.iters is not None:
        env["ITERS"] = str(args.iters)
    if args.iters_vgg16 is not None:
        env["ITERS_VGG16"] = str(args.iters_vgg16)
    if args.batches:
        env["BATCHES"] = " ".join(str(x) for x in args.batches)
    if args.workloads:
        env["WORKLOADS"] = " ".join(args.workloads)
    if args.python:
        env["PYTHON"] = args.python
    return CommandSpec(
        name="collect",
        argv=["bash", "workloads/collect_workloads.sh"],
        cwd=ROOT_DIR,
        env_overrides=env,
    )


def parse_spec(args: argparse.Namespace) -> CommandSpec:
    argv = python_cmd(
        args.python,
        ROOT_DIR / "analysis" / "parse_to_csv.py",
        ["--output-dir", args.output_data_dir, "--exclude-workloads", *args.exclude_workloads],
    )
    return CommandSpec(name="parse", argv=argv, cwd=ROOT_DIR, env_overrides={})


def analyze_spec(args: argparse.Namespace) -> CommandSpec:
    argv = python_cmd(
        args.python,
        ROOT_DIR / "analysis" / "analyze_and_plot.py",
        ["--data-dir", args.output_data_dir, "--chart-dir", args.chart_dir, "--exclude-workloads", *args.exclude_workloads],
    )
    return CommandSpec(name="analyze", argv=argv, cwd=ROOT_DIR, env_overrides={})


def sched_spec(args: argparse.Namespace) -> CommandSpec:
    m = metric_subdirs(args.chart_dir)
    argv = python_cmd(
        args.python,
        ROOT_DIR / "analysis" / "recompute_sched_metrics.py",
        ["--out-dir", m["base"], "--exclude-workloads", *args.exclude_workloads],
    )
    return CommandSpec(name="sched", argv=argv, cwd=ROOT_DIR, env_overrides={})


def validate_spec(args: argparse.Namespace) -> CommandSpec:
    m = metric_subdirs(args.chart_dir)
    argv = python_cmd(
        args.python,
        ROOT_DIR / "analysis" / "validators.py",
        ["--data-dir", args.output_data_dir, "--out-dir", m["validation"], "--min-sms", str(args.min_sms)],
    )
    return CommandSpec(name="validate", argv=argv, cwd=ROOT_DIR, env_overrides={})


def report_spec(args: argparse.Namespace) -> CommandSpec:
    m = metric_subdirs(args.chart_dir)
    argv = python_cmd(
        args.python,
        ROOT_DIR / "analysis" / "reporting.py",
        [
            "--metrics-dir",
            m["base"],
            "--validation-summary",
            f"{m['validation']}/validation_summary.json",
            "--out-dir",
            m["report"],
        ],
    )
    return CommandSpec(name="report", argv=argv, cwd=ROOT_DIR, env_overrides={})


def run_spec(spec: CommandSpec, dry_run: bool = False) -> int:
    cmd_str = " ".join(shlex.quote(x) for x in spec.argv)
    if spec.env_overrides:
        env_str = " ".join(f"{k}={shlex.quote(v)}" for k, v in sorted(spec.env_overrides.items()))
        cmd_str = f"{env_str} {cmd_str}"
    print(f"[step:{spec.name}] cwd={spec.cwd}")
    print(f"[step:{spec.name}] cmd={cmd_str}")

    if dry_run:
        return 0

    env = os.environ.copy()
    env.update(spec.env_overrides)
    proc = subprocess.run(spec.argv, cwd=spec.cwd, env=env)
    if proc.returncode != 0:
        print(f"[step:{spec.name}] failed rc={proc.returncode}")
    else:
        print(f"[step:{spec.name}] done")
    return int(proc.returncode)


def build_plan(action: str, args: argparse.Namespace) -> list[CommandSpec]:
    steps = {
        "collect": [collect_spec(args)],
        "parse": [parse_spec(args)],
        "sched": [sched_spec(args)],
        "analyze": [analyze_spec(args)],
        "validate": [validate_spec(args)],
        "report": [report_spec(args)],
        "all": [
            collect_spec(args),
            parse_spec(args),
            sched_spec(args),
            analyze_spec(args),
            validate_spec(args),
            report_spec(args),
        ],
    }
    if action not in steps:
        raise SystemExit(f"unknown action: {action}")
    return steps[action]


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run RSFAN analysis pipeline steps")
    p.add_argument(
        "action",
        choices=["collect", "parse", "sched", "analyze", "validate", "report", "all"],
        help="pipeline step to run",
    )
    p.add_argument("--python", default=sys.executable, help="python executable for python-based steps")
    p.add_argument("--output-data-dir", default="../output/data", help="data output/input directory used by parse/analyze/validate")
    p.add_argument("--chart-dir", default="../output/chart", help="chart directory used by analyze/validate/report")
    p.add_argument("--exclude-workloads", nargs="*", default=[], help="workloads excluded by parse step")
    p.add_argument("--min-sms", type=int, default=1, help="minimum unique SM count for validation")

    # Optional knobs for collect step.
    p.add_argument("--iters", type=int, default=None, help="override ITERS in collect script")
    p.add_argument("--iters-vgg16", type=int, default=None, help="override ITERS_VGG16 in collect script")
    p.add_argument("--batches", type=int, nargs="*", default=None, help="override batch list in collect script")
    p.add_argument("--workloads", nargs="*", default=None, help="override workload list in collect script")

    p.add_argument("--dry-run", action="store_true", help="print resolved commands without executing")
    p.add_argument("--continue-on-error", action="store_true", help="continue to next step when one step fails")
    p.add_argument("--config", default=None, help="optional TOML config file for pipeline settings")
    p.add_argument("--show-resolved-config", action="store_true", help="print final resolved config values before running")
    return p


def main() -> None:
    p = parser()
    args = p.parse_args()

    if args.config:
        cfg = load_pipeline_config(args.config)
        args = apply_config_to_args(args, cfg)
        if args.show_resolved_config:
            print("[config] loaded")
            print(config_to_dict(cfg))

    if not args.dry_run:
        migrate_legacy_metric_outputs(args.chart_dir)

    plan = build_plan(args.action, args)

    final_rc = 0
    for spec in plan:
        rc = run_spec(spec, dry_run=args.dry_run)
        if rc != 0:
            final_rc = rc
            if not args.continue_on_error:
                raise SystemExit(rc)

    if final_rc != 0:
        raise SystemExit(final_rc)


if __name__ == "__main__":
    main()
