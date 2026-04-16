#!/usr/bin/env python3
"""Configuration loader for RSFAN analysis pipeline.

This module centralizes configuration parsing/validation for reproducible runs.
It does not change pipeline semantics; it only supplies validated arguments.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11 fallback
    import tomli as tomllib


DEFAULT_WORKLOADS = ["compute", "memory", "mixed", "sparse", "vgg16"]
DEFAULT_EXCLUDE_WORKLOADS: list[str] = []
DEFAULT_BATCHES = [8, 16, 32, 64]


@dataclass(frozen=True)
class CollectConfig:
    iters: int | None = None
    iters_vgg16: int | None = None
    batches: list[int] = field(default_factory=lambda: list(DEFAULT_BATCHES))
    workloads: list[str] = field(default_factory=lambda: list(DEFAULT_WORKLOADS))


@dataclass(frozen=True)
class PipelineConfig:
    python: str | None = None
    output_data_dir: str = "../output/data"
    chart_dir: str = "../output/chart"
    exclude_workloads: list[str] = field(default_factory=lambda: list(DEFAULT_EXCLUDE_WORKLOADS))
    min_sms: int = 1
    collect: CollectConfig = field(default_factory=CollectConfig)


def _ensure_str_list(name: str, v: Any) -> list[str]:
    if v is None:
        return []
    if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
        raise ValueError(f"'{name}' must be a list of strings")
    return [x.strip() for x in v if x and x.strip()]


def _ensure_int_list(name: str, v: Any) -> list[int]:
    if v is None:
        return []
    if not isinstance(v, list):
        raise ValueError(f"'{name}' must be a list of integers")
    out: list[int] = []
    for x in v:
        if not isinstance(x, int):
            raise ValueError(f"'{name}' must contain only integers")
        out.append(int(x))
    return out


def _ensure_opt_int(name: str, v: Any) -> int | None:
    if v is None:
        return None
    if not isinstance(v, int):
        raise ValueError(f"'{name}' must be an integer")
    return int(v)


def _ensure_opt_str(name: str, v: Any) -> str | None:
    if v is None:
        return None
    if not isinstance(v, str):
        raise ValueError(f"'{name}' must be a string")
    s = v.strip()
    return s if s else None


def _validate_collect(c: CollectConfig) -> None:
    if c.iters is not None and c.iters <= 0:
        raise ValueError("collect.iters must be > 0")
    if c.iters_vgg16 is not None and c.iters_vgg16 <= 0:
        raise ValueError("collect.iters_vgg16 must be > 0")
    if any(b <= 0 for b in c.batches):
        raise ValueError("collect.batches values must be > 0")
    if not c.workloads:
        raise ValueError("collect.workloads cannot be empty")


def _validate_pipeline(c: PipelineConfig) -> None:
    if c.min_sms <= 0:
        raise ValueError("min_sms must be > 0")
    if not c.output_data_dir.strip():
        raise ValueError("output_data_dir cannot be empty")
    if not c.chart_dir.strip():
        raise ValueError("chart_dir cannot be empty")
    _validate_collect(c.collect)


def load_pipeline_config(path: str | Path) -> PipelineConfig:
    cfg_path = Path(path)
    with cfg_path.open("rb") as f:
        raw = tomllib.load(f)

    if not isinstance(raw, dict):
        raise ValueError("config root must be a table")

    collect_raw = raw.get("collect", {})
    if collect_raw is None:
        collect_raw = {}
    if not isinstance(collect_raw, dict):
        raise ValueError("'collect' must be a table")

    collect = CollectConfig(
        iters=_ensure_opt_int("collect.iters", collect_raw.get("iters")),
        iters_vgg16=_ensure_opt_int("collect.iters_vgg16", collect_raw.get("iters_vgg16")),
        batches=_ensure_int_list("collect.batches", collect_raw.get("batches")) or list(DEFAULT_BATCHES),
        workloads=_ensure_str_list("collect.workloads", collect_raw.get("workloads")) or list(DEFAULT_WORKLOADS),
    )

    cfg = PipelineConfig(
        python=_ensure_opt_str("python", raw.get("python")),
        output_data_dir=_ensure_opt_str("output_data_dir", raw.get("output_data_dir")) or "../output/data",
        chart_dir=_ensure_opt_str("chart_dir", raw.get("chart_dir")) or "../output/chart",
        exclude_workloads=_ensure_str_list("exclude_workloads", raw.get("exclude_workloads"))
        or list(DEFAULT_EXCLUDE_WORKLOADS),
        min_sms=_ensure_opt_int("min_sms", raw.get("min_sms")) or 1,
        collect=collect,
    )
    _validate_pipeline(cfg)
    return cfg


def apply_config_to_args(args: argparse.Namespace, cfg: PipelineConfig) -> argparse.Namespace:
    """Apply config values only when CLI values are not explicitly set.

    CLI explicit flags still take precedence.
    """

    out = argparse.Namespace(**vars(args))

    if not getattr(out, "python", None):
        out.python = cfg.python
    if getattr(out, "output_data_dir", None) in (None, "", "../output/data"):
        out.output_data_dir = cfg.output_data_dir
    if getattr(out, "chart_dir", None) in (None, "", "../output/chart"):
        out.chart_dir = cfg.chart_dir
    if getattr(out, "exclude_workloads", None) in (None, []):
        out.exclude_workloads = list(cfg.exclude_workloads)
    if getattr(out, "min_sms", None) in (None, 1):
        out.min_sms = int(cfg.min_sms)

    if getattr(out, "iters", None) is None:
        out.iters = cfg.collect.iters
    if getattr(out, "iters_vgg16", None) is None:
        out.iters_vgg16 = cfg.collect.iters_vgg16
    if getattr(out, "batches", None) is None:
        out.batches = list(cfg.collect.batches)
    if getattr(out, "workloads", None) is None:
        out.workloads = list(cfg.collect.workloads)

    # If config specified a python executable, use it when --python was omitted.
    if out.python is None:
        out.python = os.environ.get("PYTHON")

    return out


def config_to_dict(cfg: PipelineConfig) -> dict[str, Any]:
    return dataclasses.asdict(cfg)
