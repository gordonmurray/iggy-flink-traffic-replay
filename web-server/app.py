"""A shared connection pool with an optional, reproducible checkout leak."""

import asyncio
import hashlib
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Header, Response

BUGGY = os.environ.get("BUGGY", "true").lower() == "true"
POOL_SIZE = 8
POOL_TIMEOUT_SECONDS = 0.15
pool = asyncio.Semaphore(POOL_SIZE)
checkout_count = 0
active_requests = 0
app = FastAPI(title="Replay target", version="v1" if BUGGY else "v2")


@asynccontextmanager
async def connection(leak: bool = False) -> AsyncIterator[None]:
    await asyncio.wait_for(pool.acquire(), timeout=POOL_TIMEOUT_SECONDS)
    try:
        yield
    finally:
        if not leak:
            pool.release()


@app.get("/ready")
async def ready() -> dict[str, object]:
    return {"ready": True, "buggy": BUGGY}


@app.post("/reset")
async def reset() -> dict[str, int]:
    global pool, checkout_count
    if active_requests:
        raise RuntimeError("Cannot reset while requests are active")
    pool = asyncio.Semaphore(POOL_SIZE)
    checkout_count = 0
    return {"available": POOL_SIZE}


@app.get("/pool/stats")
async def stats() -> dict[str, int]:
    return {"size": POOL_SIZE, "available": pool._value}


async def handle(endpoint: str, event_id: str) -> Response:
    global checkout_count, active_requests
    active_requests += 1
    # Both versions perform the same simulated work for each captured request.
    delay = (10 + hashlib.sha256(event_id.encode()).digest()[0] % 20) / 1000
    leak = False
    if endpoint == "checkout":
        checkout_count += 1
        leak = BUGGY and checkout_count % 2 == 0
    try:
        if endpoint in {"checkout", "orders", "health"}:
            async with connection(leak):
                await asyncio.sleep(delay)
        else:
            await asyncio.sleep(delay)
        return Response('{"ok":true}', media_type="application/json")
    except asyncio.TimeoutError:
        return Response(
            '{"error":"pool timeout"}', status_code=503, media_type="application/json"
        )
    finally:
        active_requests -= 1


@app.get("/api/products")
async def products(x_event_id: str = Header()) -> Response:
    return await handle("products", x_event_id)


@app.get("/api/users")
async def users(x_event_id: str = Header()) -> Response:
    return await handle("users", x_event_id)


@app.post("/api/checkout")
async def checkout(x_event_id: str = Header()) -> Response:
    return await handle("checkout", x_event_id)


@app.post("/api/orders")
async def orders(x_event_id: str = Header()) -> Response:
    return await handle("orders", x_event_id)


@app.get("/health")
async def health(x_event_id: str = Header()) -> Response:
    return await handle("health", x_event_id)
