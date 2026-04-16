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
MG = _load_module(ROOT / "analysis" / "metrics_guard.py", "metrics_guard_test_mod")


def _delay() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"workload": "a", "batch": 8, "sched_cycles_per_sm_mean": 1.0, "dispatch_gap_p95_cycles": 2.0, "dispatch_gap_max_cycles": 3.0},
            {"workload": "b", "batch": 8, "sched_cycles_per_sm_mean": 2.0, "dispatch_gap_p95_cycles": 3.0, "dispatch_gap_max_cycles": 4.0},
        ]
    )


def _load() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"workload": "a", "batch": 8, "block_imbalance_ratio": 0.1, "elapsed_sum_cv": 0.2, "jain_block_fairness": 0.95},
            {"workload": "b", "batch": 8, "block_imbalance_ratio": 0.3, "elapsed_sum_cv": 0.4, "jain_block_fairness": 0.90},
        ]
    )


def _rank() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"workload": "b", "overall_risk_score": 6.0, "rank_delay": 2.0, "rank_imbalance": 2.0, "rank_cv": 1.0, "rank_fairness_bad": 1.0},
            {"workload": "a", "overall_risk_score": 4.0, "rank_delay": 1.0, "rank_imbalance": 1.0, "rank_cv": 2.0, "rank_fairness_bad": 0.0},
        ]
    )


class GuardCheckTests(unittest.TestCase):
    def test_pair_coverage_ok(self):
        issues = MG.check_pair_coverage(_delay(), _load())
        self.assertEqual(issues, [])

    def test_pair_coverage_mismatch(self):
        d = _delay().copy()
        l = _load().copy().iloc[:1]
        issues = MG.check_pair_coverage(d, l)
        self.assertGreaterEqual(len(issues), 1)
        self.assertTrue(any(i.check == "pair_coverage" for i in issues))

    def test_workload_coverage_detects_delay_load_mismatch(self):
        d = _delay().copy()
        l = _load().copy()
        l.loc[1, "workload"] = "c"
        issues = MG.check_workload_coverage(d, l, _rank())
        checks = {i.check for i in issues}
        self.assertIn("workload_coverage", checks)
        self.assertTrue(any(i.severity == "error" for i in issues))

    def test_workload_coverage_detects_delay_rank_mismatch_warning(self):
        r = _rank().copy().iloc[:1]
        issues = MG.check_workload_coverage(_delay(), _load(), r)
        self.assertTrue(any(i.check == "workload_coverage" and i.severity == "warning" for i in issues))

    def test_metric_ranges_negative_and_jain_out_of_range(self):
        d = _delay().copy()
        l = _load().copy()
        d.loc[0, "dispatch_gap_p95_cycles"] = -1
        l.loc[0, "jain_block_fairness"] = 1.2
        l.loc[1, "elapsed_sum_cv"] = -0.1
        issues = MG.check_metric_ranges(d, l)
        checks = {i.check for i in issues}
        self.assertIn("sched_ranges", checks)
        self.assertIn("jain_range", checks)
        self.assertIn("elapsed_cv_range", checks)

    def test_rank_consistency_detects_problems(self):
        r = _rank().copy()
        r.loc[0, "overall_risk_score"] = 999
        issues = MG.check_rank_consistency(r)
        checks = {i.check for i in issues}
        self.assertIn("rank_consistency", checks)
        self.assertIn("rank_positive", checks)

    def test_rank_order_warning_when_not_descending(self):
        r = _rank().copy().sort_values("overall_risk_score", ascending=True).reset_index(drop=True)
        issues = MG.check_rank_consistency(r)
        self.assertTrue(any(i.check == "rank_order" and i.severity == "warning" for i in issues))

    def test_run_guards_aggregates_multiple_checks(self):
        d = _delay().copy()
        l = _load().copy()
        r = _rank().copy()
        d.loc[0, "sched_cycles_per_sm_mean"] = -1
        l = l.iloc[:1]
        r.loc[0, "overall_risk_score"] = 777
        issues = MG.run_guards(d, l, r)
        checks = {i.check for i in issues}
        self.assertIn("pair_coverage", checks)
        self.assertIn("sched_ranges", checks)
        self.assertIn("rank_consistency", checks)


class SummaryAndReportTests(unittest.TestCase):
    def test_summarize(self):
        issues = [
            MG.GuardIssue("x", "error", "a"),
            MG.GuardIssue("y", "warning", "b"),
        ]
        s = MG.summarize(issues)
        self.assertFalse(s["passed"])
        self.assertEqual(s["issue_counts"]["error"], 1)
        self.assertEqual(s["issue_counts"]["warning"], 1)

    def test_write_report(self):
        issues = [MG.GuardIssue("x", "warning", "a")]
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "r.json"
            p = MG.write_report(issues, out)
            self.assertTrue(p.exists())
            data = json.loads(p.read_text(encoding="utf-8"))
            self.assertEqual(data["summary"]["total_issues"], 1)


class LoadTablesTests(unittest.TestCase):
    def test_load_tables(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            _delay().to_csv(d / "sched_summary_by_workload_batch.csv", index=False)
            _load().to_csv(d / "load_summary_by_workload_batch.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "workload": "a",
                        "overall_risk_score": 1,
                        "rank_sched": 1,
                        "rank_imbalance": 1,
                        "rank_cv": 1,
                        "rank_fairness_bad": 1,
                    }
                ]
            ).to_csv(d / "workload_risk_ranking.csv", index=False)
            dd, ll, rr = MG.load_tables(d)
            self.assertEqual(len(dd), 2)
            self.assertEqual(len(ll), 2)
            self.assertEqual(len(rr), 1)

    def test_load_tables_missing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(SystemExit):
                MG.load_tables(d)


if __name__ == "__main__":
    unittest.main(verbosity=2)
