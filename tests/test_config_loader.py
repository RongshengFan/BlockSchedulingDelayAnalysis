#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module: {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


ROOT = Path(__file__).resolve().parents[1]
CFG = _load_module(ROOT / "analysis" / "config_loader.py", "config_loader_test_mod")


class LoadConfigTests(unittest.TestCase):
    def test_load_pipeline_config_with_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "cfg.toml"
            p.write_text("", encoding="utf-8")
            c = CFG.load_pipeline_config(p)
            self.assertEqual(c.output_data_dir, "../output/data")
            self.assertEqual(c.chart_dir, "../output/chart")
            self.assertEqual(c.collect.workloads, ["compute", "memory", "mixed", "sparse", "vgg16"])

    def test_load_pipeline_config_full(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "cfg.toml"
            p.write_text(
                textwrap.dedent(
                    """
                    python = "/usr/bin/python3"
                    output_data_dir = "../x/data"
                    chart_dir = "../x/chart"
                    exclude_workloads = ["dummy"]
                    min_sms = 2

                    [collect]
                    iters = 4
                    iters_vgg16 = 1
                    batches = [8, 16]
                    workloads = ["compute", "vgg16"]
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            c = CFG.load_pipeline_config(p)
            self.assertEqual(c.python, "/usr/bin/python3")
            self.assertEqual(c.output_data_dir, "../x/data")
            self.assertEqual(c.chart_dir, "../x/chart")
            self.assertEqual(c.exclude_workloads, ["dummy"])
            self.assertEqual(c.min_sms, 2)
            self.assertEqual(c.collect.iters, 4)
            self.assertEqual(c.collect.iters_vgg16, 1)
            self.assertEqual(c.collect.batches, [8, 16])
            self.assertEqual(c.collect.workloads, ["compute", "vgg16"])

    def test_invalid_type_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "cfg.toml"
            p.write_text("min_sms = 'bad'\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                CFG.load_pipeline_config(p)


class ApplyConfigTests(unittest.TestCase):
    def test_apply_config_to_args(self):
        cfg = CFG.PipelineConfig(
            python="/usr/bin/python3",
            output_data_dir="../data2",
            chart_dir="../chart2",
            exclude_workloads=["x"],
            min_sms=3,
            collect=CFG.CollectConfig(iters=9, iters_vgg16=2, batches=[1, 2], workloads=["compute"]),
        )
        args = argparse.Namespace(
            action="all",
            python=None,
            output_data_dir="../output/data",
            chart_dir="../output/chart",
            exclude_workloads=[],
            min_sms=1,
            iters=None,
            iters_vgg16=None,
            batches=None,
            workloads=None,
            dry_run=False,
            continue_on_error=False,
            config=None,
            show_resolved_config=False,
        )
        out = CFG.apply_config_to_args(args, cfg)
        self.assertEqual(out.python, "/usr/bin/python3")
        self.assertEqual(out.output_data_dir, "../data2")
        self.assertEqual(out.chart_dir, "../chart2")
        self.assertEqual(out.exclude_workloads, ["x"])
        self.assertEqual(out.min_sms, 3)
        self.assertEqual(out.iters, 9)
        self.assertEqual(out.iters_vgg16, 2)
        self.assertEqual(out.batches, [1, 2])
        self.assertEqual(out.workloads, ["compute"])

    def test_cli_values_take_precedence(self):
        cfg = CFG.PipelineConfig(
            python="/usr/bin/python3",
            output_data_dir="../data2",
            chart_dir="../chart2",
            exclude_workloads=["x"],
            min_sms=2,
            collect=CFG.CollectConfig(iters=9, iters_vgg16=2, batches=[1, 2], workloads=["compute"]),
        )
        args = argparse.Namespace(
            action="all",
            python="/custom/python",
            output_data_dir="/tmp/d",
            chart_dir="/tmp/c",
            exclude_workloads=["keep"],
            min_sms=5,
            iters=99,
            iters_vgg16=7,
            batches=[64],
            workloads=["vgg16"],
            dry_run=False,
            continue_on_error=False,
            config=None,
            show_resolved_config=False,
        )
        out = CFG.apply_config_to_args(args, cfg)
        self.assertEqual(out.python, "/custom/python")
        self.assertEqual(out.output_data_dir, "/tmp/d")
        self.assertEqual(out.chart_dir, "/tmp/c")
        self.assertEqual(out.exclude_workloads, ["keep"])
        self.assertEqual(out.min_sms, 5)
        self.assertEqual(out.iters, 99)
        self.assertEqual(out.iters_vgg16, 7)
        self.assertEqual(out.batches, [64])
        self.assertEqual(out.workloads, ["vgg16"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
