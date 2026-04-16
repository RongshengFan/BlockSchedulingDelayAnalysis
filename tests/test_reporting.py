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
REP = _load_module(ROOT / "analysis" / "reporting.py", "reporting_test_mod")


def _sched_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "workload": "compute",
                "batch": 8,
                "sched_cycles_per_sm_mean": 2.0,
                "sched_event_count": 100,
                "sched_event_ratio": 0.5,
                "sched_mean_cycles": 2.0,
                "sched_p95_cycles": 4.0,
                "sched_max_cycles": 6.0,
            },
            {
                "workload": "memory",
                "batch": 8,
                "sched_cycles_per_sm_mean": 3.0,
                "sched_event_count": 100,
                "sched_event_ratio": 0.5,
                "sched_mean_cycles": 3.0,
                "sched_p95_cycles": 5.0,
                "sched_max_cycles": 8.0,
            },
            {
                "workload": "compute",
                "batch": 16,
                "sched_cycles_per_sm_mean": 4.0,
                "sched_event_count": 150,
                "sched_event_ratio": 0.75,
                "sched_mean_cycles": 4.0,
                "sched_p95_cycles": 7.0,
                "sched_max_cycles": 9.0,
            },
            {
                "workload": "memory",
                "batch": 16,
                "sched_cycles_per_sm_mean": 1.0,
                "sched_event_count": 150,
                "sched_event_ratio": 0.75,
                "sched_mean_cycles": 1.0,
                "sched_p95_cycles": 2.0,
                "sched_max_cycles": 3.0,
            },
        ]
    )


def _load_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "workload": "compute",
                "batch": 8,
                "sm_count": 10,
                "block_mean": 100.0,
                "block_std": 20.0,
                "block_max": 140.0,
                "block_min": 80.0,
                "elapsed_sum_mean": 10.0,
                "elapsed_sum_std": 2.0,
                "block_cv": 0.2,
                "elapsed_sum_cv": 0.2,
                "block_imbalance_ratio": 0.6,
                "jain_block_fairness": 0.90,
            },
            {
                "workload": "memory",
                "batch": 8,
                "sm_count": 10,
                "block_mean": 100.0,
                "block_std": 5.0,
                "block_max": 108.0,
                "block_min": 92.0,
                "elapsed_sum_mean": 8.0,
                "elapsed_sum_std": 1.0,
                "block_cv": 0.05,
                "elapsed_sum_cv": 0.125,
                "block_imbalance_ratio": 0.16,
                "jain_block_fairness": 0.98,
            },
            {
                "workload": "compute",
                "batch": 16,
                "sm_count": 10,
                "block_mean": 200.0,
                "block_std": 40.0,
                "block_max": 260.0,
                "block_min": 160.0,
                "elapsed_sum_mean": 20.0,
                "elapsed_sum_std": 4.0,
                "block_cv": 0.2,
                "elapsed_sum_cv": 0.2,
                "block_imbalance_ratio": 0.5,
                "jain_block_fairness": 0.88,
            },
            {
                "workload": "memory",
                "batch": 16,
                "sm_count": 10,
                "block_mean": 200.0,
                "block_std": 10.0,
                "block_max": 215.0,
                "block_min": 185.0,
                "elapsed_sum_mean": 16.0,
                "elapsed_sum_std": 1.6,
                "block_cv": 0.05,
                "elapsed_sum_cv": 0.1,
                "block_imbalance_ratio": 0.15,
                "jain_block_fairness": 0.99,
            },
        ]
    )


class LoadMetricsTests(unittest.TestCase):
    def test_load_metrics_reads_expected_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            _sched_df().to_csv(d / "sched_summary_by_workload_batch.csv", index=False)
            _load_df().to_csv(d / "load_summary_by_workload_batch.csv", index=False)
            (d / "validation_summary.json").write_text(
                json.dumps({"rows": 123, "passed": True, "issue_counts": {"error": 0, "warning": 0, "info": 0}}),
                encoding="utf-8",
            )

            delay, load, val = REP.load_metrics(d)
            self.assertEqual(len(delay), 4)
            self.assertEqual(len(load), 4)
            self.assertTrue(val["passed"])

    def test_load_metrics_missing_csv_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(SystemExit):
                REP.load_metrics(d)


class FindingsTests(unittest.TestCase):
    def test_build_delay_findings(self):
        findings = REP.build_delay_findings(_sched_df())
        self.assertGreaterEqual(len(findings), 4)
        self.assertTrue(any(f.category == "sched" for f in findings))

    def test_build_load_findings(self):
        findings = REP.build_load_findings(_load_df())
        self.assertGreaterEqual(len(findings), 4)
        self.assertTrue(any(f.category == "load" for f in findings))


class RankingTests(unittest.TestCase):
    def test_aggregate_ranking(self):
        rank = REP.aggregate_ranking(_sched_df(), _load_df())
        self.assertEqual(set(rank["workload"].tolist()), {"compute", "memory"})
        self.assertIn("overall_risk_score", rank.columns)
        self.assertIn("rank_sched", rank.columns)
        self.assertEqual(len(rank), 2)

    def test_build_conclusion(self):
        delay = _sched_df()
        load = _load_df()
        rank = REP.aggregate_ranking(delay, load)
        val = {"rows": 250, "passed": True, "issue_counts": {"error": 0, "warning": 0, "info": 0}}
        c = REP.build_conclusion(delay, load, val, rank)
        self.assertIn("sched_findings", c)
        self.assertIn("load_findings", c)
        self.assertIn("top_risk_workload", c)
        self.assertIn("metric_definitions", c)
        self.assertIn("ranking_method", c)
        self.assertIn("correlation_summary", c)
        self.assertTrue(c["correlation_summary"]["pairwise"])


class MarkdownAndSaveTests(unittest.TestCase):
    def test_to_markdown_contains_expected_sections(self):
        delay = _sched_df()
        load = _load_df()
        rank = REP.aggregate_ranking(delay, load)
        val = {"rows": 250, "passed": True, "issue_counts": {"error": 0, "warning": 0, "info": 0}}
        c = REP.build_conclusion(delay, load, val, rank)
        text = REP.to_markdown(c, rank)
        self.assertIn("Validation", text)
        self.assertIn("Sched Findings", text)
        self.assertIn("Load Findings", text)
        self.assertIn("Correlation Summary", text)
        self.assertIn("Workload Composite Ranking", text)
        self.assertIn("Conclusion", text)

    def test_save_report_writes_files(self):
        delay = _sched_df()
        load = _load_df()
        rank = REP.aggregate_ranking(delay, load)
        val = {"rows": 250, "passed": True, "issue_counts": {"error": 0, "warning": 0, "info": 0}}
        c = REP.build_conclusion(delay, load, val, rank)

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            json_path, md_path, rank_path = REP.save_report(c, rank, out_dir)
            self.assertTrue(json_path.exists())
            self.assertTrue(md_path.exists())
            self.assertTrue(rank_path.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
