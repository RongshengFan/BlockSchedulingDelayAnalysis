#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module: {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


ROOT = Path(__file__).resolve().parents[1]
PARSE_MOD = _load_module(ROOT / "analysis" / "parse_to_csv.py", "parse_to_csv_test_mod")


class InferTagsTests(unittest.TestCase):
    def test_infer_tags_basic(self):
        path = "traces/mixed/bs32/trace/3/result/run_abc.bin"
        workload, batch, trace_id, kernel_run_id = PARSE_MOD.infer_tags(path)
        self.assertEqual(workload, "mixed")
        self.assertEqual(batch, 32)
        self.assertEqual(trace_id, "3")
        self.assertEqual(kernel_run_id, "run_abc")

    def test_infer_tags_when_layout_not_matched(self):
        path = "foo/bar/result/test.bin"
        workload, batch, trace_id, kernel_run_id = PARSE_MOD.infer_tags(path)
        self.assertEqual(workload, "")
        self.assertIsNone(batch)
        self.assertEqual(trace_id, "")
        self.assertEqual(kernel_run_id, "test")


class CollectBinsTests(unittest.TestCase):
    def test_collect_bins_unique_sorted_and_bin_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            a = root / "a.bin"
            b = root / "b.bin"
            c = root / "c.txt"
            for p in [a, b, c]:
                p.write_text("x", encoding="utf-8")

            out = PARSE_MOD.collect_bins([str(root / "*"), str(root / "a.*")])
            self.assertEqual(out, [str(a), str(b)])


class BuildBlockTableTests(unittest.TestCase):
    def test_build_block_table_computes_sched_from_start_clock(self):
        df = pd.DataFrame(
            [
                {
                    "workload": "compute",
                    "batch": 8,
                    "block_id": 0,
                    "sm": 1,
                    "start_clock": 1000,
                    "start_ts": 100,
                    "elapsed": 10,
                    "_trace_id": "1",
                    "_kernel_run_id": "k1",
                },
                {
                    "workload": "compute",
                    "batch": 8,
                    "block_id": 1,
                    "sm": 1,
                    "start_clock": 1020,
                    "start_ts": 120,
                    "elapsed": 9,
                    "_trace_id": "1",
                    "_kernel_run_id": "k1",
                },
                {
                    "workload": "memory",
                    "batch": 16,
                    "block_id": 0,
                    "sm": 0,
                    "start_clock": 2000,
                    "start_ts": 200,
                    "elapsed": 5,
                    "_trace_id": "2",
                    "_kernel_run_id": "k2",
                },
            ]
        )
        out = PARSE_MOD.build_block_table(df)

        self.assertEqual(
            list(out.columns),
            ["workload", "batch", "block_id", "sm", "start_clock", "start_ts", "elapsed", "sched"],
        )

        compute = out[out["workload"] == "compute"].sort_values("block_id")
        self.assertEqual(int(compute.iloc[0]["start_clock"]), 1000)
        self.assertEqual(int(compute.iloc[1]["start_clock"]), 1020)
        self.assertEqual(int(compute.iloc[0]["sched"]), 0)
        self.assertEqual(int(compute.iloc[1]["sched"]), 10)

        memory = out[out["workload"] == "memory"].iloc[0]
        self.assertEqual(int(memory["start_clock"]), 2000)
        self.assertEqual(int(memory["sched"]), 0)


class WriteOutputsTests(unittest.TestCase):
    def test_write_outputs_removes_stale_and_writes_per_workload(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            stale = out_dir / "old.csv"
            stale.write_text("stale", encoding="utf-8")

            df = pd.DataFrame(
                [
                    {
                        "workload": "compute",
                        "batch": 8,
                        "block_id": 0,
                        "sm": 1,
                        "start_clock": 1000,
                        "start_ts": 100,
                        "elapsed": 10,
                        "sched": 0,
                    },
                    {
                        "workload": "memory",
                        "batch": 8,
                        "block_id": 1,
                        "sm": 2,
                        "start_clock": 1010,
                        "start_ts": 110,
                        "elapsed": 11,
                        "sched": 2,
                    },
                ]
            )

            PARSE_MOD.write_outputs(df, out_dir)

            self.assertFalse(stale.exists())
            self.assertTrue((out_dir / "compute.csv").exists())
            self.assertTrue((out_dir / "memory.csv").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
