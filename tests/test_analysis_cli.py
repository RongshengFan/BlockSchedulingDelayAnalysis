#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import io
import sys
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
CLI = _load_module(ROOT / "analysis" / "cli.py", "analysis_cli_test_mod")


def _args(**overrides):
    base = dict(
        action="all",
        python="python",
        output_data_dir="../output/data",
        chart_dir="../output/chart",
        exclude_workloads=[],
        min_sms=1,
        iters=None,
        iters_vgg16=None,
        batches=None,
        workloads=None,
        dry_run=True,
        continue_on_error=False,
        config=None,
        show_resolved_config=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


class CommandBuilderTests(unittest.TestCase):
    def test_python_cmd(self):
        script = Path("a.py")
        argv = CLI.python_cmd("/usr/bin/python", script, ["--x", "1"])
        self.assertEqual(argv, ["/usr/bin/python", "a.py", "--x", "1"])

    def test_collect_spec_env_overrides(self):
        args = _args(iters=4, iters_vgg16=2, batches=[8, 16], workloads=["compute", "vgg16"], python="/p")
        spec = CLI.collect_spec(args)
        self.assertEqual(spec.name, "collect")
        self.assertEqual(spec.argv, ["bash", "workloads/collect_workloads.sh"])
        self.assertEqual(spec.env_overrides["ITERS"], "4")
        self.assertEqual(spec.env_overrides["ITERS_VGG16"], "2")
        self.assertEqual(spec.env_overrides["BATCHES"], "8 16")
        self.assertEqual(spec.env_overrides["WORKLOADS"], "compute vgg16")
        self.assertEqual(spec.env_overrides["PYTHON"], "/p")

    def test_parse_spec_contains_exclude_workloads(self):
        args = _args(exclude_workloads=["dummy"])
        spec = CLI.parse_spec(args)
        cmd = " ".join(spec.argv)
        self.assertIn("parse_to_csv.py", cmd)
        self.assertIn("--exclude-workloads", cmd)
        self.assertIn("dummy", cmd)

    def test_sched_spec_targets_base_metrics_dir(self):
        args = _args(chart_dir="../output/chart", exclude_workloads=["dummy"])
        spec = CLI.sched_spec(args)
        cmd = " ".join(spec.argv)
        self.assertEqual(spec.name, "sched")
        self.assertIn("recompute_sched_metrics.py", cmd)
        self.assertIn("../output/chart/metrics/base", cmd)
        self.assertIn("--exclude-workloads", cmd)
        self.assertIn("dummy", cmd)


class PlanTests(unittest.TestCase):
    def test_build_plan_all(self):
        args = _args(action="all")
        plan = CLI.build_plan("all", args)
        self.assertEqual([s.name for s in plan], ["collect", "parse", "sched", "analyze", "validate", "report"])

    def test_build_plan_single_step(self):
        args = _args(action="validate")
        plan = CLI.build_plan("validate", args)
        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0].name, "validate")

    def test_build_plan_invalid_action(self):
        args = _args(action="all")
        with self.assertRaises(SystemExit):
            CLI.build_plan("unknown", args)


class RunSpecTests(unittest.TestCase):
    def test_run_spec_dry_run(self):
        spec = CLI.CommandSpec(name="x", argv=["echo", "hello"], cwd=Path("."), env_overrides={"A": "1"})
        rc = CLI.run_spec(spec, dry_run=True)
        self.assertEqual(rc, 0)

    def test_run_spec_exec_success(self):
        spec = CLI.CommandSpec(name="x", argv=["echo", "ok"], cwd=Path("."), env_overrides={})
        with mock.patch("analysis_cli_test_mod.subprocess.run") as run:
            run.return_value.returncode = 0
            rc = CLI.run_spec(spec, dry_run=False)
            self.assertEqual(rc, 0)
            run.assert_called_once()

    def test_run_spec_exec_failure(self):
        spec = CLI.CommandSpec(name="x", argv=["false"], cwd=Path("."), env_overrides={})
        with mock.patch("analysis_cli_test_mod.subprocess.run") as run:
            run.return_value.returncode = 3
            rc = CLI.run_spec(spec, dry_run=False)
            self.assertEqual(rc, 3)


class MainTests(unittest.TestCase):
    def test_main_dry_run_all(self):
        argv = [
            "cli.py",
            "all",
            "--python",
            "python",
            "--dry-run",
        ]
        with mock.patch.object(sys, "argv", argv):
            with mock.patch("sys.stdout", new_callable=io.StringIO) as out:
                CLI.main()
                text = out.getvalue()
                self.assertIn("[step:collect]", text)
                self.assertIn("[step:parse]", text)
                self.assertIn("[step:sched]", text)
                self.assertIn("[step:analyze]", text)
                self.assertIn("[step:validate]", text)
                self.assertIn("[step:report]", text)

    def test_main_stops_on_error_by_default(self):
        argv = ["cli.py", "all", "--python", "python"]
        with mock.patch.object(sys, "argv", argv):
            with mock.patch("analysis_cli_test_mod.run_spec", side_effect=[0, 2, 0, 0, 0, 0]):
                with self.assertRaises(SystemExit) as cm:
                    CLI.main()
                self.assertEqual(cm.exception.code, 2)

    def test_main_continue_on_error(self):
        argv = ["cli.py", "all", "--python", "python", "--continue-on-error"]
        with mock.patch.object(sys, "argv", argv):
            with mock.patch("analysis_cli_test_mod.run_spec", side_effect=[0, 2, 0, 0, 0, 0]):
                with self.assertRaises(SystemExit) as cm:
                    CLI.main()
                self.assertEqual(cm.exception.code, 2)

    def test_main_with_config_calls_loader(self):
        argv = ["cli.py", "all", "--python", "python", "--config", "cfg.toml", "--dry-run"]
        fake_args = _args()
        with mock.patch.object(sys, "argv", argv):
            with mock.patch("analysis_cli_test_mod.load_pipeline_config") as load_cfg:
                with mock.patch("analysis_cli_test_mod.apply_config_to_args", return_value=fake_args) as apply_cfg:
                    with mock.patch("analysis_cli_test_mod.run_spec", return_value=0):
                        CLI.main()
                        load_cfg.assert_called_once_with("cfg.toml")
                        apply_cfg.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
