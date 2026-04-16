#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
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
VAL = _load_module(ROOT / "analysis" / "validators.py", "validators_edge_test_mod")


def _base_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "workload": "compute",
                "batch": 8,
                "block_id": 0,
                "sm": 0,
                "launch_anchor_ts": 10,
                "start_ts": 10,
                "launch_offset": 0,
                "elapsed": 3,
                "sched": 0,
            },
            {
                "workload": "compute",
                "batch": 8,
                "block_id": 1,
                "sm": 1,
                "launch_anchor_ts": 10,
                "start_ts": 15,
                "launch_offset": 5,
                "elapsed": 4,
                "sched": 1,
            },
            {
                "workload": "memory",
                "batch": 16,
                "block_id": 0,
                "sm": 0,
                "launch_anchor_ts": 20,
                "start_ts": 20,
                "launch_offset": 0,
                "elapsed": 6,
                "sched": 0,
            },
        ]
    )


class NullAndSchemaTests(unittest.TestCase):
    def test_check_nulls_ignores_missing_columns(self):
        df = pd.DataFrame([{"a": 1}])
        issues = VAL.check_nulls(df, ["x", "y"])
        self.assertEqual(issues, [])

    def test_check_nulls_emits_issues(self):
        df = _base_df().copy()
        df.loc[1, "launch_offset"] = None
        issues = VAL.check_nulls(df, ["launch_offset"])
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].check, "null_values")

    def test_validate_dataframe_with_missing_schema_only_returns_required_columns(self):
        df = pd.DataFrame([{"workload": "x", "batch": 1, "launch_offset": 0}])
        issues = VAL.validate_dataframe(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].check, "required_columns")


class DelayAndTimingTests(unittest.TestCase):
    def test_delay_mismatch_and_negative_detected_together(self):
        df = _base_df().copy()
        df.loc[1, "launch_anchor_ts"] = 30
        df.loc[1, "launch_offset"] = -20
        issues = VAL.check_delay_definition(df)
        checks = {i.check for i in issues}
        self.assertIn("negative_launch_offset", checks)

    def test_ready_not_after_start(self):
        df = _base_df().copy()
        df.loc[0, "launch_anchor_ts"] = 99
        issues = VAL.check_ready_not_after_start(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].check, "launch_anchor_after_start")


class DuplicateTests(unittest.TestCase):
    def test_duplicate_full_row_warns_once_per_group(self):
        df = _base_df().copy()
        df = pd.concat([df, df.iloc[[0]], df.iloc[[0]]], ignore_index=True)
        issues = VAL.check_duplicate_blocks(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].severity, "warning")

    def test_no_duplicate_when_any_field_differs(self):
        df = _base_df().copy()
        extra = df.iloc[[0]].copy()
        extra.loc[extra.index[0], "elapsed"] = 99
        df = pd.concat([df, extra], ignore_index=True)
        issues = VAL.check_duplicate_blocks(df)
        self.assertEqual(issues, [])


class CoverageAndSummaryTests(unittest.TestCase):
    def test_sm_coverage_with_higher_threshold(self):
        df = _base_df().copy()
        issues = VAL.check_sm_coverage(df, min_sms=3)
        self.assertGreaterEqual(len(issues), 1)
        self.assertTrue(all(i.check == "sm_coverage" for i in issues))

    def test_compute_summary_pass_and_fail(self):
        df = _base_df().copy()
        s1 = VAL.compute_summary(df, [])
        self.assertTrue(s1["passed"])

        issues = [
            VAL.ValidationIssue("x", "error", "compute", 8, 0, "bad"),
            VAL.ValidationIssue("y", "warning", "compute", 8, None, "warn"),
        ]
        s2 = VAL.compute_summary(df, issues)
        self.assertFalse(s2["passed"])
        self.assertEqual(s2["issue_counts"]["error"], 1)
        self.assertEqual(s2["issue_counts"]["warning"], 1)


class ValidationFlowTests(unittest.TestCase):
    def test_validate_dataframe_multiple_error_types(self):
        df = _base_df().copy()
        df.loc[0, "elapsed"] = 0
        df.loc[1, "launch_anchor_ts"] = 999
        df.loc[1, "launch_offset"] = -984
        df.loc[1, "sched"] = -1
        issues = VAL.validate_dataframe(df, min_sms=2)
        checks = {i.check for i in issues}
        self.assertIn("elapsed_positive", checks)
        self.assertIn("negative_launch_offset", checks)
        self.assertIn("negative_sched", checks)
        self.assertIn("launch_anchor_after_start", checks)

    def test_validate_dataframe_clean_input(self):
        df = _base_df().copy()
        issues = VAL.validate_dataframe(df)
        self.assertEqual(issues, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
