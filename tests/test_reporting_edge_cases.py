#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
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
REP = _load_module(ROOT / "analysis" / "reporting.py", "reporting_edge_test_mod")


def _sched_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "workload": "a",
                "batch": 8,
                "sched_cycles_per_sm_mean": 1,
                "sched_event_count": 1,
                "sched_event_ratio": 0.5,
                "sched_mean_cycles": 1,
                "sched_p95_cycles": 10,
                "sched_max_cycles": 20,
            },
            {
                "workload": "b",
                "batch": 8,
                "sched_cycles_per_sm_mean": 1,
                "sched_event_count": 1,
                "sched_event_ratio": 0.5,
                "sched_mean_cycles": 1,
                "sched_p95_cycles": 5,
                "sched_max_cycles": 7,
            },
        ]
    )


def _load_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"workload": "a", "batch": 8, "sm_count": 4, "block_mean": 10, "block_imbalance_ratio": 0.6, "elapsed_sum_cv": 0.5, "jain_block_fairness": 0.90},
            {"workload": "b", "batch": 8, "sm_count": 4, "block_mean": 10, "block_imbalance_ratio": 0.1, "elapsed_sum_cv": 0.1, "jain_block_fairness": 0.99},
        ]
    )


class LoadAndValidationTests(unittest.TestCase):
    def test_load_metrics_with_missing_json_uses_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            _sched_df().to_csv(d / "sched_summary_by_workload_batch.csv", index=False)
            _load_df().to_csv(d / "load_summary_by_workload_batch.csv", index=False)
            delay, load, val = REP.load_metrics(d)
            self.assertEqual(len(delay), 2)
            self.assertEqual(len(load), 2)
            self.assertIn("passed", val)

    def test_load_metrics_missing_column_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            pd.DataFrame([{"workload": "x"}]).to_csv(d / "sched_summary_by_workload_batch.csv", index=False)
            _load_df().to_csv(d / "load_summary_by_workload_batch.csv", index=False)
            with self.assertRaises(SystemExit):
                REP.load_metrics(d)


class RankingAndConclusionTests(unittest.TestCase):
    def test_aggregate_ranking_contains_expected_columns(self):
        rank = REP.aggregate_ranking(_sched_df(), _load_df())
        self.assertIn("overall_risk_score", rank.columns)
        self.assertIn("rank_sched", rank.columns)
        self.assertEqual(len(rank), 2)

    def test_build_conclusion_with_non_empty_rank(self):
        rank = REP.aggregate_ranking(_sched_df(), _load_df())
        val = {"rows": 2, "passed": True, "issue_counts": {"error": 0, "warning": 0, "info": 0}}
        c = REP.build_conclusion(_sched_df(), _load_df(), val, rank)
        self.assertIn("top_risk_workload", c)
        self.assertIn("lowest_risk_workload", c)
        self.assertEqual(c["n_workloads"], 2)
        self.assertIn("ranking_method", c)
        self.assertIn("correlation_summary", c)

    def test_build_conclusion_with_empty_rank(self):
        val = {"rows": 0, "passed": False, "issue_counts": {"error": 1, "warning": 0, "info": 0}}
        c = REP.build_conclusion(_sched_df().iloc[:0], _load_df().iloc[:0], val, pd.DataFrame())
        self.assertEqual(c["n_workloads"], 0)
        self.assertEqual(c["correlation_summary"]["merged_rows"], 0)


class MarkdownAndSaveTests(unittest.TestCase):
    def test_to_markdown_with_empty_rank(self):
        val = {"rows": 0, "passed": False, "issue_counts": {"error": 1, "warning": 0, "info": 0}}
        c = {
            "validation": val,
            "sched_findings": [],
            "load_findings": [],
            "top_risk_workload": {},
            "lowest_risk_workload": {},
            "n_workloads": 0,
        }
        text = REP.to_markdown(c, pd.DataFrame())
        self.assertIn("no ranking data", text)
        self.assertIn("insufficient data", text)

    def test_save_report_roundtrip(self):
        rank = REP.aggregate_ranking(_sched_df(), _load_df())
        val = {"rows": 2, "passed": True, "issue_counts": {"error": 0, "warning": 0, "info": 0}}
        c = REP.build_conclusion(_sched_df(), _load_df(), val, rank)
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            jp, mp, rp = REP.save_report(c, rank, d)
            self.assertTrue(jp.exists())
            self.assertTrue(mp.exists())
            self.assertTrue(rp.exists())
            parsed = json.loads(jp.read_text(encoding="utf-8"))
            self.assertIn("validation", parsed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
