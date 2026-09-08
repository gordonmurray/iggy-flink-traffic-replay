"""Generate request inputs once. Both HTTP runs read these inputs from Iggy."""

import asyncio
import hashlib
import json
import random
import time
import uuid
from typing import Any

import httpx

ENDPOINTS = [
    ("GET", "/api/products"),
    ("POST", "/api/orders"),
    ("GET", "/api/users"),
    ("POST", "/api/checkout"),
    ("GET", "/health"),
]


def generate_requests(
    experiment_id: str, count: int, rps: int, seed: int
) -> list[bytes]:
    if not 100 <= count <= 5000:
        raise ValueError("REQUEST_COUNT must be between 100 and 5000")
    if not 1 <= rps <= 100:
        raise ValueError("TRAFFIC_RATE_RPS must be between 1 and 100")
    rng = random.Random(seed)
    requests = []
    for sequence in range(count):
        # Each group contains every endpoint; shuffle without losing coverage.
        if sequence % len(ENDPOINTS) == 0:
            group = rng.sample(ENDPOINTS, len(ENDPOINTS))
        method, endpoint = group[sequence % len(ENDPOINTS)]
        event = {
            "event_id": str(uuid.uuid5(uuid.UUID(experiment_id), str(sequence))),
            "sequence_no": sequence,
            "scheduled_offset_ms": sequence * 1000.0 / rps,
            "method": method,
            "endpoint": endpoint,
        }
        requests.append(
            json.dumps(event, sort_keys=True, separators=(",", ":")).encode()
        )
    return requests


def capture_hash(capture: list[tuple[int, bytes]]) -> str:
    digest = hashlib.sha256()
    for offset, payload in capture:
        digest.update(offset.to_bytes(8, "big"))
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


async def execute_capture(
    capture: list[tuple[int, bytes]],
    target: str,
    experiment_id: str,
    run_id: str,
) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(
        base_url=target,
        timeout=3.0,
        limits=httpx.Limits(max_connections=256, max_keepalive_connections=128),
    ) as client:
        reset = await client.post("/reset")
        reset.raise_for_status()
        start = time.monotonic()

        async def execute(offset: int, payload: bytes) -> dict[str, Any]:
            event = json.loads(payload)
            deadline = start + event["scheduled_offset_ms"] / 1000.0
            await asyncio.sleep(max(0, deadline - time.monotonic()))
            dispatched = time.monotonic()
            status, size, error = 0, 0, ""
            try:
                response = await client.request(
                    event["method"],
                    event["endpoint"],
                    headers={"X-Event-Id": event["event_id"]},
                )
                status, size = response.status_code, len(response.content)
                if status >= 400:
                    error = response.text
            except httpx.HTTPError as exc:
                error = type(exc).__name__
            return {
                **event,
                "experiment_id": experiment_id,
                "run_id": run_id,
                "request_offset": offset,
                "request_sha256": hashlib.sha256(payload).hexdigest(),
                "dispatch_lag_ms": round((dispatched - deadline) * 1000, 3),
                "status_code": status,
                "response_time_ms": round((time.monotonic() - dispatched) * 1000, 3),
                "response_size_bytes": size,
                "error": error,
            }

        # gather returns capture order even when HTTP completions arrive out of order.
        return await asyncio.gather(
            *(execute(offset, payload) for offset, payload in capture)
        )
