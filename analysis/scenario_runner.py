#!/usr/bin/env python3
"""Scenario matrix runner for RSFAN pipeline.

This utility builds experiment scenarios from a TOML matrix and invokes the
existing analysis CLI (`analysis/cli.py`) per scenario.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import tomllib

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent


@dataclass(frozen=True)
class Scenario:
    name: str
    action: str
    config: str | None
    workloads: list[str] | None
    batches: list[int] | None
    iters: int | None
    iters_vgg16: int | None
    continue_on_error: bool
    dry_run: bool


@dataclass(frozen=True)
class ScenarioResult:
    name: str
    command: list[str]
    return_code: int
    skipped: bool
    reason: str


def _str_list(v: Any) -> list[str] | None:
    if v is None:
        return None
    if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
        raise ValueError("expected string list")
    return [x.strip() for x in v if x and x.strip()]


def _int_list(v: Any) -> list[int] | None:
    if v is None:
        return None
    if not isinstance(v, list):
        raise ValueError("expected integer list")
    out: list[int] = []
    for x in v:
        if not isinstance(x, int):
            raise ValueError("expected integer list")
        out.append(int(x))
    return out


def _opt_int(v: Any) -> int | None:
    if v is None:
        return None
    if not isinstance(v, int):
        raise ValueError("expected integer")
    return int(v)


def _opt_str(v: Any) -> str | None:
    if v is None:
        return None
    if not isinstance(v, str):
        raise ValueError("expected string")
    s = v.strip()
    return s if s else None


def parse_matrix(path: Path) -> list[Scenario]:
    with path.open("rb") as f:
        raw = tomllib.load(f)

    scenarios_raw = raw.get("scenario", [])
    if not isinstance(scenarios_raw, list):
        raise ValueError("'scenario' must be an array of tables")

    scenarios: list[Scenario] = []
    for i, s in enumerate(scenarios_raw):
        if not isinstance(s, dict):
            raise ValueError(f"scenario[{i}] must be a table")
        name = _opt_str(s.get("name")) or f"scenario_{i+1}"
        action = _opt_str(s.get("action")) or "all"
        if action not in {"collect", "parse", "analyze", "validate", "report", "all"}:
            raise ValueError(f"scenario[{i}] invalid action: {action}")

        dry_run = bool(s.get("dry_run", False))
        cont = bool(s.get("continue_on_error", False))
        scenario = Scenario(
            name=name,
            action=action,
            config=_opt_str(s.get("config")),
            workloads=_str_list(s.get("workloads")),
            batches=_int_list(s.get("batches")),
            iters=_opt_int(s.get("iters")),
            iters_vgg16=_opt_int(s.get("iters_vgg16")),
            continue_on_error=cont,
            dry_run=dry_run,
        )
        scenarios.append(scenario)

    if not scenarios:
        raise ValueError("no scenarios found")
    return scenarios


def build_cli_command(s: Scenario, python: str) -> list[str]:
    cmd = [python, str(ROOT_DIR / "analysis" / "cli.py"), s.action]
    if s.config:
        cmd += ["--config", s.config]
    if s.workloads:
        cmd += ["--workloads", *s.workloads]
    if s.batches:
        cmd += ["--batches", *[str(x) for x in s.batches]]
    if s.iters is not None:
        cmd += ["--iters", str(s.iters)]
    if s.iters_vgg16 is not None:
        cmd += ["--iters-vgg16", str(s.iters_vgg16)]
    if s.continue_on_error:
        cmd += ["--continue-on-error"]
    if s.dry_run:
        cmd += ["--dry-run"]
    cmd += ["--python", python]
    return cmd


def run_scenarios(
    scenarios: list[Scenario],
    python: str,
    global_dry_run: bool = False,
    stop_on_failure: bool = True,
) -> list[ScenarioResult]:
    results: list[ScenarioResult] = []

    for sc in scenarios:
        cmd = build_cli_command(sc, python)
        print(f"[scenario] {sc.name}")
        print(f"[scenario] cmd={' '.join(shlex.quote(x) for x in cmd)}")

        if global_dry_run:
            results.append(ScenarioResult(sc.name, cmd, 0, True, "global_dry_run"))
            continue

        proc = subprocess.run(cmd, cwd=ROOT_DIR)
        rc = int(proc.returncode)
        if rc == 0:
            results.append(ScenarioResult(sc.name, cmd, rc, False, ""))
            continue

        results.append(ScenarioResult(sc.name, cmd, rc, False, "command_failed"))
        if stop_on_failure and not sc.continue_on_error:
            break

    return results


def summarize(results: list[ScenarioResult]) -> dict[str, Any]:
    ok = sum(1 for r in results if r.return_code == 0)
    fail = sum(1 for r in results if r.return_code != 0)
    skipped = sum(1 for r in results if r.skipped)
    first_fail = next((r.name for r in results if r.return_code != 0), "")
    return {
        "total": len(results),
        "ok": ok,
        "failed": fail,
        "skipped": skipped,
        "first_failed": first_fail,
    }


def write_summary(results: list[ScenarioResult], out_file: Path) -> Path:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "summary": summarize(results),
        "results": [
            {
                "name": r.name,
                "command": r.command,
                "return_code": r.return_code,
                "skipped": r.skipped,
                "reason": r.reason,
            }
            for r in results
        ],
    }
    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Run scenario matrix by calling analysis/cli.py")
    parser.add_argument("--matrix", required=True, help="TOML matrix file path")
    parser.add_argument("--python", default="python", help="python executable")
    parser.add_argument("--dry-run", action="store_true", help="print scenario commands without executing")
    parser.add_argument(
        "--summary-out",
        default="../output/chart/metrics/scenario/scenario_matrix_summary.json",
        help="summary json output path",
    )
    parser.add_argument("--keep-going", action="store_true", help="do not stop on first failing scenario")
    args = parser.parse_args()

    matrix = Path(args.matrix)
    if not matrix.is_absolute():
        matrix = (ROOT_DIR / matrix).resolve()

    scenarios = parse_matrix(matrix)
    results = run_scenarios(
        scenarios,
        python=args.python,
        global_dry_run=bool(args.dry_run),
        stop_on_failure=not bool(args.keep_going),
    )

    out = Path(args.summary_out)
    if not out.is_absolute():
        out = (BASE_DIR / out).resolve()
    out = write_summary(results, out)
    s = summarize(results)

    print(f"[matrix] total={s['total']} ok={s['ok']} failed={s['failed']} skipped={s['skipped']}")
    print(f"[matrix] summary={out}")

    if s["failed"] > 0:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
