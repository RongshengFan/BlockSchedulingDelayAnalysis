#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module: {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


ROOT = Path(__file__).resolve().parents[1]
SR = _load_module(ROOT / "analysis" / "scenario_runner.py", "scenario_runner_test_mod")


class ParseMatrixTests(unittest.TestCase):
    def test_parse_matrix_basic(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "m.toml"
            p.write_text(
                textwrap.dedent(
                    """
                    [[scenario]]
                    name = "s1"
                    action = "all"
                    dry_run = true

                    [[scenario]]
                    action = "collect"
                    workloads = ["compute", "vgg16"]
                    batches = [8, 16]
                    iters = 2
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            out = SR.parse_matrix(p)
            self.assertEqual(len(out), 2)
            self.assertEqual(out[0].name, "s1")
            self.assertEqual(out[0].action, "all")
            self.assertTrue(out[0].dry_run)
            self.assertEqual(out[1].name, "scenario_2")
            self.assertEqual(out[1].workloads, ["compute", "vgg16"])

    def test_parse_matrix_invalid_action(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "m.toml"
            p.write_text(
                textwrap.dedent(
                    """
                    [[scenario]]
                    action = "invalid"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                SR.parse_matrix(p)

    def test_parse_matrix_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "m.toml"
            p.write_text("", encoding="utf-8")
            with self.assertRaises(ValueError):
                SR.parse_matrix(p)


class BuildCommandTests(unittest.TestCase):
    def test_build_cli_command(self):
        s = SR.Scenario(
            name="x",
            action="collect",
            config="configs/pipeline.default.toml",
            workloads=["compute", "vgg16"],
            batches=[8, 16],
            iters=3,
            iters_vgg16=1,
            continue_on_error=True,
            dry_run=True,
        )
        cmd = SR.build_cli_command(s, "/usr/bin/python")
        joined = " ".join(cmd)
        self.assertIn("analysis/cli.py", joined)
        self.assertIn("collect", cmd)
        self.assertIn("--config", cmd)
        self.assertIn("--workloads", cmd)
        self.assertIn("--batches", cmd)
        self.assertIn("--iters", cmd)
        self.assertIn("--iters-vgg16", cmd)
        self.assertIn("--continue-on-error", cmd)
        self.assertIn("--dry-run", cmd)


class RunScenarioTests(unittest.TestCase):
    def test_run_scenarios_global_dry_run(self):
        sc = [
            SR.Scenario("a", "all", None, None, None, None, None, False, False),
            SR.Scenario("b", "report", None, None, None, None, None, False, True),
        ]
        out = SR.run_scenarios(sc, python="python", global_dry_run=True, stop_on_failure=True)
        self.assertEqual(len(out), 2)
        self.assertTrue(all(x.skipped for x in out))
        self.assertTrue(all(x.return_code == 0 for x in out))

    def test_run_scenarios_stop_on_failure(self):
        sc = [
            SR.Scenario("a", "all", None, None, None, None, None, False, False),
            SR.Scenario("b", "all", None, None, None, None, None, False, False),
            SR.Scenario("c", "all", None, None, None, None, None, False, False),
        ]

        class _P:
            def __init__(self, rc):
                self.returncode = rc

        with mock.patch("scenario_runner_test_mod.subprocess.run", side_effect=[_P(0), _P(2), _P(0)]) as run:
            out = SR.run_scenarios(sc, python="python", global_dry_run=False, stop_on_failure=True)
            self.assertEqual(len(out), 2)
            self.assertEqual(out[1].return_code, 2)
            self.assertEqual(run.call_count, 2)

    def test_run_scenarios_keep_going_if_continue_on_error(self):
        sc = [
            SR.Scenario("a", "all", None, None, None, None, None, False, False),
            SR.Scenario("b", "all", None, None, None, None, None, True, False),
            SR.Scenario("c", "all", None, None, None, None, None, False, False),
        ]

        class _P:
            def __init__(self, rc):
                self.returncode = rc

        with mock.patch("scenario_runner_test_mod.subprocess.run", side_effect=[_P(0), _P(2), _P(0)]) as run:
            out = SR.run_scenarios(sc, python="python", global_dry_run=False, stop_on_failure=True)
            self.assertEqual(len(out), 3)
            self.assertEqual(run.call_count, 3)


class SummaryTests(unittest.TestCase):
    def test_summarize(self):
        rs = [
            SR.ScenarioResult("a", ["cmd"], 0, False, ""),
            SR.ScenarioResult("b", ["cmd"], 2, False, "command_failed"),
            SR.ScenarioResult("c", ["cmd"], 0, True, "global_dry_run"),
        ]
        s = SR.summarize(rs)
        self.assertEqual(s["total"], 3)
        self.assertEqual(s["ok"], 2)
        self.assertEqual(s["failed"], 1)
        self.assertEqual(s["skipped"], 1)
        self.assertEqual(s["first_failed"], "b")

    def test_write_summary(self):
        rs = [
            SR.ScenarioResult("a", ["cmd"], 0, False, ""),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "x" / "summary.json"
            out = SR.write_summary(rs, p)
            self.assertTrue(out.exists())
            data = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(data["summary"]["total"], 1)
            self.assertEqual(data["results"][0]["name"], "a")


if __name__ == "__main__":
    unittest.main(verbosity=2)
