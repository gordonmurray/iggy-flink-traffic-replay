"""Acceptance checks against committed Iceberg rows, queried with DuckDB."""

import json
from pathlib import Path
from typing import Any

import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo


def load_rows(experiment_id: str):
    catalog = load_catalog(
        "demo",
        type="rest",
        uri="http://iceberg-rest:8181",
        **{
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "demo-admin",
            "s3.secret-access-key": "demo-password",
            "s3.region": "us-east-1",
            "s3.force-virtual-addressing": "false",
        },
    )
    table = catalog.load_table("replay.traffic_runs")
    return table.scan(row_filter=EqualTo("experiment_id", experiment_id)).to_arrow()


def evaluate(rows, manifest: dict[str, Any]) -> dict[str, Any]:
    db = duckdb.connect()
    try:
        db.register("traffic", rows)
        metrics = {}
        for run_id in ("original", "replay"):
            row = db.execute(
                """
                SELECT count(*), count(DISTINCT event_id),
                       count(*) FILTER (WHERE status_code != 200),
                       count(*) FILTER (WHERE is_anomaly),
                       count(*) FILTER (WHERE status_code = 0),
                       count(*) FILTER (WHERE status_code = 503),
                       count(*) FILTER (WHERE endpoint = '/health' AND status_code != 200),
                       quantile_cont(response_time_ms, 0.95),
                       quantile_cont(dispatch_lag_ms, 0.95), max(dispatch_lag_ms)
                FROM traffic WHERE run_id = ?
            """,
                [run_id],
            ).fetchone()
            metrics[run_id] = dict(
                zip(
                    (
                        "rows",
                        "unique_events",
                        "errors",
                        "anomalies",
                        "transport_errors",
                        "pool_timeouts",
                        "health_errors",
                        "latency_p95_ms",
                        "dispatch_lag_p95_ms",
                        "dispatch_lag_max_ms",
                    ),
                    row,
                )
            )
        mismatches = db.execute("""
            SELECT count(*) FROM
              (SELECT * FROM traffic WHERE run_id = 'original') o
            FULL OUTER JOIN
              (SELECT * FROM traffic WHERE run_id = 'replay') r USING (event_id)
            WHERE o.event_id IS NULL OR r.event_id IS NULL
               OR o.request_sha256 IS DISTINCT FROM r.request_sha256
               OR o.request_offset IS DISTINCT FROM r.request_offset
               OR o.sequence_no IS DISTINCT FROM r.sequence_no
               OR o.endpoint IS DISTINCT FROM r.endpoint
               OR o.method IS DISTINCT FROM r.method
               OR o.scheduled_offset_ms IS DISTINCT FROM r.scheduled_offset_ms
        """).fetchone()[0]
        nulls = db.execute("""
            SELECT count(*) FROM traffic
            WHERE is_anomaly IS NULL OR status_code IS NULL OR request_sha256 IS NULL
               OR response_time_ms IS NULL OR dispatch_lag_ms IS NULL
               OR request_offset IS NULL OR sequence_no IS NULL
               OR endpoint IS NULL OR method IS NULL OR scheduled_offset_ms IS NULL
        """).fetchone()[0]
        original, replay = metrics["original"], metrics["replay"]
        checks = {
            "all_responses_committed": rows.num_rows == manifest["request_count"] * 2
            and all(m["rows"] == manifest["request_count"] for m in metrics.values()),
            "no_duplicate_events": all(
                m["unique_events"] == m["rows"] for m in metrics.values()
            ),
            "same_requests_and_offsets": mismatches == 0
            and manifest["capture_sha256"]
            == manifest["original_sha256"]
            == manifest["replay_sha256"],
            "no_null_measurements": nulls == 0,
            "no_transport_failures": all(
                m["transport_errors"] == 0 for m in metrics.values()
            ),
            "original_pool_failure_observed": original["pool_timeouts"] >= 10
            and original["health_errors"] > 0,
            "fixed_server_has_no_errors": replay["errors"] == 0,
            "anomalies_reduced_at_least_80_percent": original["anomalies"] > 0
            and replay["anomalies"] <= original["anomalies"] * 0.2,
            "dispatch_lag_below_250ms": all(
                m["dispatch_lag_max_ms"] is not None and m["dispatch_lag_max_ms"] < 250
                for m in metrics.values()
            ),
            "flink_completed_checkpoint": manifest.get("completed_checkpoints", 0) > 0,
            "flink_did_not_restart": manifest.get("flink_restarts") == 0,
        }
        return {
            "experiment_id": manifest["experiment_id"],
            "passed": all(checks.values()),
            "checks": checks,
            "metrics": metrics,
        }
    finally:
        db.close()


def save_report(report: dict[str, Any], directory: Path) -> str:
    lines = [
        "# Replay results",
        "",
        f"Experiment: `{report['experiment_id']}`",
        "",
        "| Check | Result |",
        "|---|---|",
    ]
    lines += [
        f"| {name} | {'PASS' if passed else 'FAIL'} |"
        for name, passed in report["checks"].items()
    ]
    lines += ["", "| Metric | Original | Replay |", "|---|---:|---:|"]
    for key in report["metrics"]["original"]:
        values = [report["metrics"][run][key] for run in ("original", "replay")]
        formatted = [f"{v:.3f}" if isinstance(v, float) else str(v) for v in values]
        lines.append(f"| {key} | {formatted[0]} | {formatted[1]} |")
    rendered = "\n".join(lines) + "\n"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (directory / "report.md").write_text(rendered)
    return rendered
