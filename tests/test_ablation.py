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
ABL = _load_module(ROOT / "analysis" / "ablation.py", "ablation_test_mod")


def _delay_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"workload": "compute", "batch": 8, "sched_p95_cycles": 10.0},
            {"workload": "memory", "batch": 8, "sched_p95_cycles": 5.0},
            {"workload": "vgg16", "batch": 8, "sched_p95_cycles": 20.0},
            {"workload": "compute", "batch": 16, "sched_p95_cycles": 12.0},
            {"workload": "memory", "batch": 16, "sched_p95_cycles": 4.0},
            {"workload": "vgg16", "batch": 16, "sched_p95_cycles": 22.0},
        ]
    )


def _load_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"workload": "compute", "batch": 8, "block_imbalance_ratio": 0.3, "elapsed_sum_cv": 0.2, "jain_block_fairness": 0.95},
            {"workload": "memory", "batch": 8, "block_imbalance_ratio": 0.1, "elapsed_sum_cv": 0.1, "jain_block_fairness": 0.99},
            {"workload": "vgg16", "batch": 8, "block_imbalance_ratio": 0.4, "elapsed_sum_cv": 0.3, "jain_block_fairness": 0.90},
            {"workload": "compute", "batch": 16, "block_imbalance_ratio": 0.35, "elapsed_sum_cv": 0.25, "jain_block_fairness": 0.93},
            {"workload": "memory", "batch": 16, "block_imbalance_ratio": 0.12, "elapsed_sum_cv": 0.11, "jain_block_fairness": 0.98},
            {"workload": "vgg16", "batch": 16, "block_imbalance_ratio": 0.45, "elapsed_sum_cv": 0.32, "jain_block_fairness": 0.89},
        ]
    )


class RankingTests(unittest.TestCase):
    def test_aggregate_ranking(self):
        rank = ABL.aggregate_ranking(_delay_df(), _load_df())
        self.assertEqual(len(rank), 3)
        self.assertIn("overall_risk_score", rank.columns)
        self.assertEqual(rank.iloc[0]["workload"], "vgg16")


class ExclusionTests(unittest.TestCase):
    def test_run_exclusion_scenarios(self):
        summary, details = ABL.run_exclusion_scenarios(_delay_df(), _load_df(), ["compute", "unknown"])
        self.assertEqual(len(summary), 2)
        self.assertTrue((summary["scenario"] == "exclude_compute").any())
        self.assertTrue((summary["scenario"] == "exclude_unknown").any())
        self.assertGreaterEqual(len(details), 1)

    def test_write_outputs(self):
        summary, details = ABL.run_exclusion_scenarios(_delay_df(), _load_df(), ["compute"])
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            s, d, m = ABL.write_outputs(summary, details, out_dir)
            self.assertTrue(s.exists())
            self.assertTrue(d.exists())
            self.assertTrue(m.exists())


class MetricLoadTests(unittest.TestCase):
    def test_load_metric_tables(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            _delay_df().to_csv(p / "sched_summary_by_workload_batch.csv", index=False)
            _load_df().to_csv(p / "load_summary_by_workload_batch.csv", index=False)
            d, l = ABL.load_metric_tables(p)
            self.assertEqual(len(d), 6)
            self.assertEqual(len(l), 6)

    def test_load_metric_tables_missing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            with self.assertRaises(SystemExit):
                ABL.load_metric_tables(p)


if __name__ == "__main__":
    unittest.main(verbosity=2)
