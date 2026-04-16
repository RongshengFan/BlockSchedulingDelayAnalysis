#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = ROOT_DIR / "output"
STATIC_DIR = Path(__file__).resolve().parent / "static"
EXCLUDED_BATCHES = {8}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


class MetricsRepository:
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.data_dir = output_dir / "data"
        self.base_dir = output_dir / "chart" / "metrics" / "base"
        self.report_dir = output_dir / "chart" / "metrics" / "report"
        self.validation_dir = output_dir / "chart" / "metrics" / "validation"
        self.chart_dir = output_dir / "chart"

    def available_workloads(self) -> list[str]:
        workloads = sorted(path.stem for path in self.data_dir.glob("*.csv"))
        if workloads:
            return workloads
        detail = self.sched_detail()
        if detail.empty:
            return []
        return sorted(detail["workload"].dropna().astype(str).unique().tolist())

    def available_batches(self) -> list[int]:
        candidates = [
            self.sched_summary(),
            self.load_summary(),
        ]
        for df in candidates:
            if not df.empty and "batch" in df.columns:
                values = pd.to_numeric(df["batch"], errors="coerce").dropna().astype(int).unique().tolist()
                values = [value for value in values if value not in EXCLUDED_BATCHES]
                return sorted(values)
        return []

    def sched_summary(self) -> pd.DataFrame:
        return _read_csv(self.base_dir / "sched_summary_by_workload_batch.csv")

    def load_summary(self) -> pd.DataFrame:
        return _read_csv(self.base_dir / "load_summary_by_workload_batch.csv")

    def ranking(self) -> pd.DataFrame:
        return _read_csv(self.report_dir / "workload_risk_ranking.csv")

    def sched_detail(self) -> pd.DataFrame:
        return _read_csv(self.base_dir / "sched_block_detail.csv")

    def load_per_sm(self) -> pd.DataFrame:
        return _read_csv(self.base_dir / "load_per_sm.csv")

    def block_detail(self, workload: str | None = None) -> pd.DataFrame:
        files = sorted(self.data_dir.glob("*.csv"))
        if workload:
            files = [self.data_dir / f"{workload}.csv"] if (self.data_dir / f"{workload}.csv").exists() else []
        if not files:
            return pd.DataFrame()
        return pd.concat([pd.read_csv(path) for path in files], ignore_index=True)

    def report(self) -> dict[str, Any]:
        path = self.report_dir / "analysis_conclusion.json"
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def validation_summary(self) -> dict[str, Any]:
        path = self.validation_dir / "validation_summary.json"
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def chart_gallery(self) -> list[dict[str, Any]]:
        groups: list[dict[str, Any]] = []
        for category in ["overview", "sched", "load"]:
            category_dir = self.chart_dir / category
            if not category_dir.exists():
                continue
            images = []
            for path in sorted(category_dir.glob("*.png")):
                images.append(
                    {
                        "name": path.name,
                        "title": path.stem.replace("_", " "),
                        "url": f"/chart-files/{category}/{path.name}",
                    }
                )
            groups.append({"category": category, "count": len(images), "images": images})
        return groups


def _normalize_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    clean = df.copy()
    clean = clean.where(pd.notnull(clean), None)
    return clean.to_dict(orient="records")


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _sanitize_report(report: dict[str, Any]) -> dict[str, Any]:
    if not report:
        return {}

    def _filter_findings(items: Any) -> Any:
        if not isinstance(items, list):
            return items
        out: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            batch = item.get("batch")
            if batch is not None:
                try:
                    if int(batch) in EXCLUDED_BATCHES:
                        continue
                except (TypeError, ValueError):
                    pass
            out.append(item)
        return out

    clean = dict(report)
    clean["sched_findings"] = _filter_findings(clean.get("sched_findings"))
    clean["delay_findings"] = _filter_findings(clean.get("delay_findings"))
    clean["load_findings"] = _filter_findings(clean.get("load_findings"))

    validation = clean.get("validation")
    if isinstance(validation, dict) and "batches" in validation:
        batches = validation.get("batches") or []
        if isinstance(batches, list):
            filtered: list[int] = []
            for b in batches:
                try:
                    bi = int(b)
                except (TypeError, ValueError):
                    continue
                if bi in EXCLUDED_BATCHES:
                    continue
                filtered.append(bi)
            new_validation = dict(validation)
            new_validation["batches"] = filtered
            clean["validation"] = new_validation

    return clean


class DashboardHandler(SimpleHTTPRequestHandler):
    repo: MetricsRepository

    def __init__(self, *args: Any, directory: str | None = None, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        body = _json_bytes(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path) -> None:
        if not path.exists() or not path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return
        data = path.read_bytes()
        content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _query_filters(self) -> tuple[list[str], list[int]]:
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        workloads = [item for item in params.get("workload", []) if item]
        batches: list[int] = []
        for value in params.get("batch", []):
            try:
                batches.append(int(value))
            except ValueError:
                continue
        batches = [batch for batch in batches if batch not in EXCLUDED_BATCHES]
        return workloads, batches

    def _default_batches(self, batches: list[int]) -> list[int]:
        if batches:
            return batches
        return self.repo.available_batches()

    @staticmethod
    def _apply_filters(df: pd.DataFrame, workloads: list[str], batches: list[int]) -> pd.DataFrame:
        if df.empty:
            return df
        out = df.copy()
        if workloads and "workload" in out.columns:
            out = out[out["workload"].astype(str).isin(workloads)]
        if batches and "batch" in out.columns:
            out = out[pd.to_numeric(out["batch"], errors="coerce").isin(batches)]
        return out

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/favicon.ico":
            self.send_response(HTTPStatus.NO_CONTENT)
            self.end_headers()
            return

        if parsed.path == "/api/meta":
            self._send_json(
                {
                    "workloads": self.repo.available_workloads(),
                    "batches": self.repo.available_batches(),
                    "outputDir": str(self.repo.output_dir),
                }
            )
            return

        if parsed.path == "/api/summary":
            workloads, batches = self._query_filters()
            batches = self._default_batches(batches)
            payload = {
                "sched": _normalize_records(self._apply_filters(self.repo.sched_summary(), workloads, batches)),
                "load": _normalize_records(self._apply_filters(self.repo.load_summary(), workloads, batches)),
                "ranking": _normalize_records(self.repo.ranking()),
                "validation": self.repo.validation_summary(),
            }
            self._send_json(payload)
            return

        if parsed.path == "/api/distribution":
            workloads, batches = self._query_filters()
            batches = self._default_batches(batches)
            metric = parse_qs(parsed.query).get("metric", ["sched"])[0]
            if metric != "sched":
                self._send_json({"error": "unsupported metric", "metric": metric, "records": []}, status=HTTPStatus.BAD_REQUEST)
                return

            df = self.repo.sched_detail()
            value_col = "sched"

            df = self._apply_filters(df, workloads, batches)
            if not df.empty and value_col in df.columns:
                df = df[["workload", "batch", value_col]].copy()
                df = df.rename(columns={value_col: "value"})
            else:
                df = pd.DataFrame(columns=["workload", "batch", "value"])
            self._send_json({"metric": metric, "records": _normalize_records(df.head(5000))})
            return

        if parsed.path == "/api/heatmap":
            workloads, batches = self._query_filters()
            batches = self._default_batches(batches)
            df = self._apply_filters(self.repo.load_per_sm(), workloads, batches)
            self._send_json({"records": _normalize_records(df)})
            return

        if parsed.path == "/api/report":
            self._send_json({"report": _sanitize_report(self.repo.report())})
            return

        if parsed.path == "/api/gallery":
            self._send_json({"groups": self.repo.chart_gallery()})
            return

        if parsed.path.startswith("/chart-files/"):
            relative = parsed.path.removeprefix("/chart-files/")
            target = (self.repo.chart_dir / relative).resolve()
            chart_root = self.repo.chart_dir.resolve()
            if chart_root not in target.parents and target != chart_root:
                self.send_error(HTTPStatus.FORBIDDEN, "Forbidden")
                return
            self._send_file(target)
            return

        return super().do_GET()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local dashboard for BlockSchedulingDelayAnalysis metrics")
    parser.add_argument("--host", default="127.0.0.1", help="host to bind")
    parser.add_argument("--port", type=int, default=8765, help="port to bind")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="existing output directory")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo = MetricsRepository(Path(args.output_dir).resolve())
    handler_cls = DashboardHandler
    handler_cls.repo = repo
    server = ThreadingHTTPServer((args.host, args.port), handler_cls)
    print(f"[dashboard] serving http://{args.host}:{args.port}")
    print(f"[dashboard] reading metrics from {repo.output_dir}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
