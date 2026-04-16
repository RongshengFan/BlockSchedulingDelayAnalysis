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
VAL = _load_module(ROOT / "analysis" / "validators.py", "validators_test_mod")


def _good_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "workload": "compute",
                "batch": 8,
                "block_id": 0,
                "sm": 0,
                "launch_anchor_ts": 100,
                "start_ts": 100,
                "launch_offset": 0,
                "elapsed": 20,
                "sched": 0,
            },
            {
                "workload": "compute",
                "batch": 8,
                "block_id": 1,
                "sm": 1,
                "launch_anchor_ts": 100,
                "start_ts": 120,
                "launch_offset": 20,
                "elapsed": 25,
                "sched": 5,
            },
            {
                "workload": "memory",
                "batch": 16,
                "block_id": 0,
                "sm": 0,
                "launch_anchor_ts": 200,
                "start_ts": 200,
                "launch_offset": 0,
                "elapsed": 10,
                "sched": 0,
            },
        ]
    )


class RequiredColumnsTests(unittest.TestCase):
    def test_missing_columns_raise_required_columns_issue(self):
        df = pd.DataFrame([{"workload": "compute"}])
        issues = VAL.check_required_columns(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].check, "required_columns")
        self.assertEqual(issues[0].severity, "error")


class DelayChecksTests(unittest.TestCase):
    def test_delay_definition_passes_for_good_data(self):
        df = _good_df()
        issues = VAL.check_delay_definition(df)
        self.assertEqual(issues, [])

    def test_delay_definition_catches_mismatch(self):
        df = _good_df().copy()
        df.loc[1, "launch_offset"] = 1
        issues = VAL.check_delay_definition(df)
        self.assertTrue(any(i.check == "launch_offset_definition" for i in issues))

    def test_delay_definition_catches_negative_delay(self):
        df = _good_df().copy()
        df.loc[1, "launch_anchor_ts"] = 500
        df.loc[1, "launch_offset"] = -380
        issues = VAL.check_delay_definition(df)
        self.assertTrue(any(i.check == "negative_launch_offset" for i in issues))


class ElapsedChecksTests(unittest.TestCase):
    def test_elapsed_positive_catches_zero_or_negative(self):
        df = _good_df().copy()
        df.loc[0, "elapsed"] = 0
        df.loc[1, "elapsed"] = -3
        issues = VAL.check_elapsed_positive(df)
        self.assertEqual(len(issues), 2)
        self.assertTrue(all(i.check == "elapsed_positive" for i in issues))

    def test_sched_non_negative_catches_negative(self):
        df = _good_df().copy()
        df.loc[1, "sched"] = -1
        issues = VAL.check_sched_non_negative(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].check, "negative_sched")


class ReadyStartChecksTests(unittest.TestCase):
    def test_ready_not_after_start_passes_for_good_data(self):
        df = _good_df()
        issues = VAL.check_ready_not_after_start(df)
        self.assertEqual(issues, [])

    def test_ready_not_after_start_catches_violations(self):
        df = _good_df().copy()
        df.loc[1, "launch_anchor_ts"] = 130
        issues = VAL.check_ready_not_after_start(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].check, "launch_anchor_after_start")


class DuplicateAndCoverageTests(unittest.TestCase):
    def test_duplicate_blocks_warn(self):
        df = _good_df().copy()
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        issues = VAL.check_duplicate_blocks(df)
        self.assertGreaterEqual(len(issues), 1)
        self.assertTrue(all(i.check == "duplicate_blocks" for i in issues))

    def test_sm_coverage_warns_with_threshold(self):
        df = _good_df().copy()
        # Keep single SM for memory group and ask for at least 2.
        issues = VAL.check_sm_coverage(df, min_sms=2)
        self.assertTrue(any(i.check == "sm_coverage" for i in issues))

    def test_duplicate_blocks_does_not_warn_for_same_block_id_with_different_timing(self):
        df = _good_df().copy()
        # Reusing block_id is legal across multiple launches; only exact row duplicates should warn.
        extra = df.iloc[[0]].copy()
        extra.loc[extra.index[0], "start_ts"] = 140
        extra.loc[extra.index[0], "launch_offset"] = 40
        df = pd.concat([df, extra], ignore_index=True)
        issues = VAL.check_duplicate_blocks(df)
        self.assertEqual(issues, [])


class ValidateDataframeTests(unittest.TestCase):
    def test_validate_dataframe_passes_good_df(self):
        df = _good_df()
        issues = VAL.validate_dataframe(df)
        self.assertEqual(issues, [])

    def test_validate_dataframe_short_circuit_on_schema_error(self):
        df = pd.DataFrame([{"workload": "compute", "batch": 8}])
        issues = VAL.validate_dataframe(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].check, "required_columns")


class SummaryAndReportTests(unittest.TestCase):
    def test_compute_summary(self):
        df = _good_df()
        issues = [
            VAL.ValidationIssue("x", "warning", "compute", 8, None, "warn"),
            VAL.ValidationIssue("y", "error", "memory", 16, 2, "err"),
        ]
        summary = VAL.compute_summary(df, issues)
        self.assertEqual(summary["rows"], 3)
        self.assertEqual(summary["issue_counts"]["warning"], 1)
        self.assertEqual(summary["issue_counts"]["error"], 1)
        self.assertFalse(summary["passed"])

    def test_save_reports_outputs_files(self):
        df = _good_df()
        issues = [
            VAL.ValidationIssue("x", "warning", "compute", 8, None, "warn"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            summary_json, issue_csv, issue_md = VAL.save_reports(df, issues, out_dir)
            self.assertTrue(summary_json.exists())
            self.assertTrue(issue_csv.exists())
            self.assertTrue(issue_md.exists())


class FileLoadTests(unittest.TestCase):
    def test_load_per_workload_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            _good_df().to_csv(d / "compute.csv", index=False)
            _good_df().iloc[:1].to_csv(d / "memory.csv", index=False)
            out = VAL.load_per_workload_csv(d)
            self.assertEqual(len(out), 4)

    def test_load_per_workload_csv_raises_for_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(SystemExit):
                VAL.load_per_workload_csv(d)


if __name__ == "__main__":
    unittest.main(verbosity=2)
