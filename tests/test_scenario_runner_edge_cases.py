#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import io
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
SR = _load_module(ROOT / "analysis" / "scenario_runner.py", "scenario_runner_edge_test_mod")


class ParseTypeValidationTests(unittest.TestCase):
    def test_invalid_workloads_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "m.toml"
            p.write_text(
                textwrap.dedent(
                    """
                    [[scenario]]
                    action = "collect"
                    workloads = "compute"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                SR.parse_matrix(p)

    def test_invalid_batches_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "m.toml"
            p.write_text(
                textwrap.dedent(
                    """
                    [[scenario]]
                    action = "collect"
                    batches = [8, "16"]
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                SR.parse_matrix(p)

    def test_invalid_iters_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "m.toml"
            p.write_text(
                textwrap.dedent(
                    """
                    [[scenario]]
                    action = "collect"
                    iters = "x"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                SR.parse_matrix(p)


class MainFlowTests(unittest.TestCase):
    def test_main_dry_run_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            matrix = Path(tmp) / "m.toml"
            matrix.write_text(
                textwrap.dedent(
                    """
                    [[scenario]]
                    name = "s1"
                    action = "all"
                    dry_run = true
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            out = Path(tmp) / "summary.json"
            argv = [
                "scenario_runner.py",
                "--matrix",
                str(matrix),
                "--python",
                "python",
                "--dry-run",
                "--summary-out",
                str(out),
            ]
            with mock.patch.object(sys, "argv", argv):
                with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                    SR.main()
                    text = stdout.getvalue()
                    self.assertIn("[matrix] total=1", text)
                    self.assertTrue(out.exists())

    def test_main_failure_exit_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            matrix = Path(tmp) / "m.toml"
            matrix.write_text(
                textwrap.dedent(
                    """
                    [[scenario]]
                    name = "s1"
                    action = "all"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            class _P:
                def __init__(self, rc):
                    self.returncode = rc

            argv = ["scenario_runner.py", "--matrix", str(matrix), "--python", "python"]
            with mock.patch.object(sys, "argv", argv):
                with mock.patch("scenario_runner_edge_test_mod.subprocess.run", return_value=_P(2)):
                    with self.assertRaises(SystemExit) as cm:
                        SR.main()
                    self.assertEqual(cm.exception.code, 2)


class SummaryPayloadTests(unittest.TestCase):
    def test_summary_json_shape(self):
        results = [
            SR.ScenarioResult("s1", ["python", "x"], 0, False, ""),
            SR.ScenarioResult("s2", ["python", "y"], 1, False, "command_failed"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "summary.json"
            SR.write_summary(results, out)
            data = json.loads(out.read_text(encoding="utf-8"))
            self.assertIn("summary", data)
            self.assertIn("results", data)
            self.assertEqual(data["summary"]["failed"], 1)
            self.assertEqual(data["results"][1]["reason"], "command_failed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
