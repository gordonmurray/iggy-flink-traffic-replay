import asyncio
import hashlib
import json
import sys
from pathlib import Path

import httpx
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from report import evaluate
from traffic_generator import capture_hash, execute_capture, generate_requests

EXPERIMENT = "12345678123456781234567812345678"


def fixture_rows():
    payloads = generate_requests(EXPERIMENT, 100, 40, 42)
    capture = list(enumerate(payloads))
    rows = []
    for run in ("original", "replay"):
        for offset, payload in capture:
            event = json.loads(payload)
            failed = run == "original" and event["endpoint"] in {
                "/health",
                "/api/checkout",
            }
            rows.append(
                {
                    **event,
                    "run_id": run,
                    "request_offset": offset,
                    "request_sha256": hashlib.sha256(payload).hexdigest(),
                    "status_code": 503 if failed else 200,
                    "response_time_ms": 160.0 if failed else 20.0,
                    "dispatch_lag_ms": 1.0,
                    "is_anomaly": failed,
                }
            )
    digest = capture_hash(capture)
    manifest = {
        "experiment_id": EXPERIMENT,
        "request_count": 100,
        "capture_sha256": digest,
        "original_sha256": digest,
        "replay_sha256": digest,
        "completed_checkpoints": 2,
        "flink_restarts": 0,
    }
    return rows, manifest


def test_capture_is_reproducible_and_covers_all_endpoints():
    first = generate_requests(EXPERIMENT, 100, 40, 42)
    assert first == generate_requests(EXPERIMENT, 100, 40, 42)
    assert len({json.loads(p)["event_id"] for p in first}) == 100
    assert len({json.loads(p)["endpoint"] for p in first[:5]}) == 5
    assert json.loads(first[-1])["scheduled_offset_ms"] == 2475.0


def test_capture_hash_covers_bytes_offsets_and_order():
    capture = [(0, b"a"), (1, b"b")]
    original = capture_hash(capture)
    assert original != capture_hash([(0, b"a"), (1, b"c")])
    assert original != capture_hash([(10, b"a"), (11, b"b")])
    assert original != capture_hash(list(reversed(capture)))


@pytest.mark.parametrize("count,rps", [(0, 40), (5001, 40), (100, 0), (100, 101)])
def test_invalid_workload_fails_before_contacting_services(count, rps):
    with pytest.raises(ValueError):
        generate_requests(EXPERIMENT, count, rps, 42)


def test_report_passes_complete_matching_runs():
    rows, manifest = fixture_rows()
    assert evaluate(pa.Table.from_pylist(rows), manifest)["passed"]


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "duplicate",
        "payload",
        "offset",
        "schedule",
        "fixed_failure",
        "fixed_4xx",
        "transport",
        "capture",
        "slow_dispatch",
        "no_checkpoint",
        "restart",
        "no_original_failure",
    ],
)
def test_report_rejects_broken_experiment(damage):
    rows, manifest = fixture_rows()
    if damage == "missing":
        rows.pop()
    elif damage == "duplicate":
        rows[-1] = rows[-2].copy()
    elif damage == "payload":
        rows[-1]["request_sha256"] = "wrong"
    elif damage == "offset":
        rows[-1]["request_offset"] += 1
    elif damage == "schedule":
        rows[-1]["scheduled_offset_ms"] += 1
    elif damage == "fixed_failure":
        rows[-1]["status_code"] = 503
    elif damage == "fixed_4xx":
        rows[-1]["status_code"] = 422
    elif damage == "transport":
        rows[0]["status_code"] = 0
    elif damage == "capture":
        manifest["capture_sha256"] = "wrong"
    elif damage == "slow_dispatch":
        rows[-1]["dispatch_lag_ms"] = 300
    elif damage == "no_checkpoint":
        manifest["completed_checkpoints"] = 0
    elif damage == "restart":
        manifest["flink_restarts"] = 1
    elif damage == "no_original_failure":
        for row in rows:
            row["status_code"] = 200
            row["is_anomaly"] = False
    assert not evaluate(pa.Table.from_pylist(rows), manifest)["passed"]


def test_http_execution_uses_captured_method_path_id_and_measures_response(monkeypatch):
    seen = []

    def respond(request):
        if request.url.path == "/reset":
            return httpx.Response(200, json={"available": 8})
        seen.append((request.method, request.url.path, request.headers["X-Event-Id"]))
        return httpx.Response(503, text="pool timeout")

    original_client = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original_client(
            transport=httpx.MockTransport(respond), **kwargs
        ),
    )
    payload = json.dumps(
        {
            "event_id": "event-1",
            "sequence_no": 0,
            "scheduled_offset_ms": 0,
            "method": "POST",
            "endpoint": "/api/checkout",
        }
    ).encode()
    rows = asyncio.run(
        execute_capture([(17, payload)], "http://target", EXPERIMENT, "original")
    )
    assert seen == [("POST", "/api/checkout", "event-1")]
    assert rows[0]["status_code"] == 503
    assert rows[0]["request_offset"] == 17
    assert rows[0]["request_sha256"] == hashlib.sha256(payload).hexdigest()
    assert rows[0]["error"] == "pool timeout"
    assert rows[0]["response_size_bytes"] == 12


@pytest.mark.parametrize("changed", [False, True])
def test_retained_capture_check_rejects_changed_broker_data(monkeypatch, changed):
    import main

    capture = [(0, b"saved-request")]
    manifest = {
        "stream": "saved-stream",
        "start_offset": 0,
        "request_count": 1,
        "capture_sha256": capture_hash(capture),
    }

    class SavedBroker:
        async def connect(self):
            pass

        async def read(self, stream, start, count):
            assert (stream, start, count) == ("saved-stream", 0, 1)
            return [(0, b"changed-request")] if changed else capture

    monkeypatch.setattr(main, "Broker", SavedBroker)
    if changed:
        with pytest.raises(RuntimeError, match="does not match"):
            asyncio.run(main.check_retained_capture(manifest))
    else:
        asyncio.run(main.check_retained_capture(manifest))
