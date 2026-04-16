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
SCHED = _load_module(ROOT / "analysis" / "recompute_sched_metrics.py", "sched_metrics_test_mod")


class SimulateSchedTests(unittest.TestCase):
    def test_simulate_sm_sched_events_detects_replacement(self):
        df = pd.DataFrame(
            [
                {
                    "workload": "compute",
                    "batch": 8,
                    "trace_id": "t1",
                    "kernel_run_id": "k1",
                    "block_id": 0,
                    "sm": 0,
                    "start_clock": 0,
                    "start_time": 1000,
                    "elapsed": 10,
                    "end_clock": 10,
                },
                {
                    "workload": "compute",
                    "batch": 8,
                    "trace_id": "t1",
                    "kernel_run_id": "k1",
                    "block_id": 1,
                    "sm": 0,
                    "start_clock": 3,
                    "start_time": 1003,
                    "elapsed": 12,
                    "end_clock": 15,
                },
                {
                    "workload": "compute",
                    "batch": 8,
                    "trace_id": "t1",
                    "kernel_run_id": "k1",
                    "block_id": 2,
                    "sm": 0,
                    "start_clock": 18,
                    "start_time": 1018,
                    "elapsed": 8,
                    "end_clock": 26,
                },
            ]
        )

        detail, events = SCHED.simulate_sm_sched_events(df)

        self.assertEqual(len(detail), 1)
        self.assertEqual(len(events), 1)
        self.assertEqual(int(detail.iloc[0]["sched_event_count"]), 1)
        self.assertEqual(int(detail.iloc[0]["sched_cycles_total"]), 3)
        self.assertEqual(int(detail.iloc[0]["inferred_slot_count"]), 2)
        self.assertEqual(int(events.iloc[0]["sched"]), 3)

    def test_build_sched_tables_summarizes_by_workload_batch(self):
        df = pd.DataFrame(
            [
                {
                    "workload": "memory",
                    "batch": 16,
                    "trace_id": "t1",
                    "kernel_run_id": "k1",
                    "block_id": 0,
                    "sm": 1,
                    "start_clock": 0,
                    "start_time": 0,
                    "elapsed": 5,
                    "end_clock": 5,
                },
                {
                    "workload": "memory",
                    "batch": 16,
                    "trace_id": "t1",
                    "kernel_run_id": "k1",
                    "block_id": 1,
                    "sm": 1,
                    "start_clock": 9,
                    "start_time": 9,
                    "elapsed": 4,
                    "end_clock": 13,
                },
                {
                    "workload": "memory",
                    "batch": 16,
                    "trace_id": "t1",
                    "kernel_run_id": "k2",
                    "block_id": 0,
                    "sm": 2,
                    "start_clock": 0,
                    "start_time": 0,
                    "elapsed": 7,
                    "end_clock": 7,
                },
            ]
        )

        detail, events, summary = SCHED.build_sched_tables(df)

        self.assertEqual(len(detail), 2)
        self.assertEqual(len(events), 1)
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary.iloc[0]["workload"], "memory")
        self.assertEqual(int(summary.iloc[0]["sched_event_count"]), 1)
        self.assertEqual(int(summary.iloc[0]["sched_max_cycles"]), 4)


class WriteOutputsTests(unittest.TestCase):
    def test_write_outputs_creates_expected_files(self):
        detail = pd.DataFrame([{"workload": "compute", "batch": 8, "trace_id": "t1", "kernel_run_id": "k1", "sm": 0, "block_count": 1, "sched_event_count": 0, "sched_cycles_total": 0, "sched_mean_cycles": 0.0, "sched_p95_cycles": 0.0, "sched_max_cycles": 0, "work_cycles_total": 10, "block_elapsed_mean_cycles": 10.0, "block_elapsed_p95_cycles": 10.0, "inferred_slot_count": 1}])
        events = pd.DataFrame([{"workload": "compute", "batch": 8, "trace_id": "t1", "kernel_run_id": "k1", "sm": 0, "prev_block_id": 0, "block_id": 1, "prev_end_clock": 10, "start_clock": 12, "sched": 2}])
        summary = pd.DataFrame([{"workload": "compute", "batch": 8, "trace_count": 1, "kernel_run_count": 1, "sm_observed": 1, "block_count_total": 1, "sched_event_count": 1, "sched_event_ratio": 1.0, "sched_cycles_per_sm_mean": 0.0, "sched_cycles_per_sm_p95": 0.0, "sched_mean_cycles": 2.0, "sched_p95_cycles": 2.0, "sched_max_cycles": 2, "work_cycles_per_sm_mean": 10.0, "block_elapsed_mean_cycles": 10.0, "block_elapsed_p95_cycles": 10.0, "inferred_slot_count_mean": 1.0, "inferred_slot_count_max": 1}])

        with tempfile.TemporaryDirectory() as tmp:
            SCHED.write_outputs(detail, events, summary, Path(tmp))
            self.assertTrue((Path(tmp) / "sched_detail_by_sm.csv").exists())
            self.assertTrue((Path(tmp) / "sched_events.csv").exists())
            self.assertTrue((Path(tmp) / "sched_summary_by_workload_batch.csv").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
