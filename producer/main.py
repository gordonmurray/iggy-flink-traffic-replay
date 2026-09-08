"""One experiment: durable capture -> real HTTP original/replay -> Flink -> Iceberg."""

import asyncio
import json
import fcntl
import logging
import os
import signal
from pathlib import Path
import sys
import time
import uuid

import httpx
from pyiceberg.exceptions import NoSuchTableError

from iggy_client import Broker
from report import evaluate, load_rows, save_report
from traffic_generator import capture_hash, execute_capture, generate_requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("demo")
RESULTS = Path("/results")
FLINK = "http://flink-jobmanager:8081"


def write_manifest(manifest: dict) -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


async def job_details(client: httpx.AsyncClient, job_id: str) -> dict:
    response = await client.get(f"/jobs/{job_id}")
    response.raise_for_status()
    details = response.json()
    if details["state"] in {"FAILED", "CANCELED", "FINISHED", "RESTARTING"}:
        errors = await client.get(f"/jobs/{job_id}/exceptions")
        raise RuntimeError(f"Flink job is {details['state']}: {errors.text}")
    return details


async def submit(client: httpx.AsyncClient, stream: str) -> str:
    async with asyncio.timeout(90):
        while True:
            response = await client.get("/overview")
            response.raise_for_status()
            if response.json().get("slots-available", 0) >= 1:
                break
            await asyncio.sleep(1)
    with Path("/opt/job/replay-job.jar").open("rb") as jar:
        response = await client.post(
            "/jars/upload",
            files={"jarfile": ("replay-job.jar", jar, "application/java-archive")},
        )
    response.raise_for_status()
    jar_id = response.json()["filename"].rsplit("/", 1)[-1]
    response = await client.post(
        f"/jars/{jar_id}/run", json={"programArgsList": [stream], "parallelism": 1}
    )
    response.raise_for_status()
    return response.json()["jobid"]


async def wait_for_rows(client: httpx.AsyncClient, manifest: dict):
    deadline = time.monotonic() + 120
    observed = -1
    while time.monotonic() < deadline:
        await job_details(client, manifest["flink_job_id"])
        try:
            rows = await asyncio.to_thread(load_rows, manifest["experiment_id"])
        except NoSuchTableError:
            await asyncio.sleep(1)
            continue
        if rows.num_rows != observed:
            observed = rows.num_rows
            log.info(
                "Iceberg has %d/%d responses", observed, manifest["request_count"] * 2
            )
        if rows.num_rows >= manifest["request_count"] * 2:
            return rows
        await asyncio.sleep(2)
    raise TimeoutError(
        f"Iceberg did not receive all responses in 120s (last count {observed})"
    )


async def run() -> bool:
    task = asyncio.current_task()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    experiment_id = uuid.uuid4().hex
    count = int(os.environ.get("REQUEST_COUNT", "400"))
    rps = int(os.environ.get("TRAFFIC_RATE_RPS", "40"))
    seed = int(os.environ.get("TRAFFIC_SEED", "42"))
    payloads = generate_requests(experiment_id, count, rps, seed)
    stream = f"replay-{experiment_id}"
    manifest = {
        "experiment_id": experiment_id,
        "stream": stream,
        "partition": 0,
        "start_offset": 0,
        "end_offset_exclusive": count,
        "request_count": count,
        "traffic_rate_rps": rps,
        "seed": seed,
        "status": "running",
    }
    write_manifest(manifest)
    for name in ("report.json", "report.md"):
        (RESULTS / name).unlink(missing_ok=True)
    broker = Broker()
    await broker.connect()
    await broker.create_capture(stream)
    await broker.send(stream, "requests", payloads)
    capture = await broker.read(stream, 0, count)
    if [payload for _, payload in capture] != payloads:
        raise RuntimeError("Iggy capture differs from submitted requests")
    manifest["capture_sha256"] = capture_hash(capture)
    log.info(
        "Captured %d requests in %s/requests, offsets [0, %d)", count, stream, count
    )
    job_id = None
    async with httpx.AsyncClient(base_url=FLINK, timeout=90) as flink:
        try:
            job_id = await submit(flink, stream)
            manifest["flink_job_id"] = job_id
            write_manifest(manifest)
            async with asyncio.timeout(90):
                while (await job_details(flink, job_id))["state"] != "RUNNING":
                    await asyncio.sleep(1)
            for run_id, target in (
                ("original", "http://web-v1:8080"),
                ("replay", "http://web-v2:8080"),
            ):
                # Always seek back to the saved offset; never regenerate replay inputs.
                capture = await broker.read(stream, manifest["start_offset"], count)
                digest = capture_hash(capture)
                if digest != manifest["capture_sha256"]:
                    raise RuntimeError("Stored request capture changed")
                manifest[f"{run_id}_sha256"] = digest
                log.info("Sending %s requests to %s", run_id, target)
                responses = await execute_capture(
                    capture, target, experiment_id, run_id
                )
                await broker.send(
                    stream, "responses", [json.dumps(row).encode() for row in responses]
                )
                write_manifest(manifest)
            rows = await wait_for_rows(flink, manifest)
            response = await flink.get(f"/jobs/{job_id}/checkpoints")
            response.raise_for_status()
            manifest["completed_checkpoints"] = response.json()["counts"]["completed"]
            response = await flink.get(
                f"/jobs/{job_id}/metrics", params={"get": "fullRestarts"}
            )
            response.raise_for_status()
            metrics = {item["id"]: item["value"] for item in response.json()}
            manifest["flink_restarts"] = int(metrics.get("fullRestarts", "-1"))
            manifest["status"] = "complete"
            report = evaluate(rows, manifest)
            manifest["passed"] = report["passed"]
            write_manifest(manifest)
            print(save_report(report, RESULTS))
            return report["passed"]
        except BaseException as exc:
            manifest["status"] = "failed"
            manifest["error"] = f"{type(exc).__name__}: {exc}"
            write_manifest(manifest)
            raise
        finally:
            if job_id:
                # Cancel only the job created by this invocation; retain captured data.
                response = await flink.patch(
                    f"/jobs/{job_id}", params={"mode": "cancel"}
                )
                if response.is_error:
                    log.error("Could not cancel demo job %s: %s", job_id, response.text)


async def check_retained_capture(manifest: dict) -> None:
    broker = Broker()
    await broker.connect()
    capture = await broker.read(
        manifest["stream"], manifest["start_offset"], manifest["request_count"]
    )
    if capture_hash(capture) != manifest["capture_sha256"]:
        raise RuntimeError("Retained Iggy request capture does not match the manifest")
    log.info("Retained Iggy request capture matches the manifest")


def command() -> None:
    if len(sys.argv) == 2 and sys.argv[1] == "report":
        manifest = json.loads((RESULTS / "manifest.json").read_text())
        if manifest["status"] != "complete":
            raise RuntimeError("Last experiment did not complete; run make run first")
        asyncio.run(check_retained_capture(manifest))
        report = evaluate(load_rows(manifest["experiment_id"]), manifest)
        print(save_report(report, RESULTS))
        passed = report["passed"]
    elif len(sys.argv) == 1:
        passed = asyncio.run(run())
    else:
        raise SystemExit("Usage: python main.py [report]")
    raise SystemExit(0 if passed else 1)


def main() -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    # Both versions share a resettable pool. Reject overlapping experiments.
    with (RESULTS / ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(
                "Another demo or report is active in this results directory"
            )
        try:
            command()
        except BaseException as exc:
            path = RESULTS / "manifest.json"
            if path.exists():
                manifest = json.loads(path.read_text())
                if manifest.get("status") == "running":
                    manifest.update(status="failed", error=str(exc))
                    write_manifest(manifest)
            raise


if __name__ == "__main__":
    main()
