"""Explicit-offset, single-partition capture for the pinned Iggy 0.7.0 SDK."""

import asyncio

from apache_iggy import IggyClient, PollingStrategy, SendMessage


class Broker:
    def __init__(self) -> None:
        self.client = IggyClient.from_connection_string("iggy://iggy:iggy@iggy:8090")

    async def connect(self) -> None:
        async with asyncio.timeout(60):
            while True:
                try:
                    await self.client.connect()
                    return
                except Exception:
                    await asyncio.sleep(1)

    async def create_capture(self, stream: str) -> None:
        # A UUID stream avoids collisions and makes each run independently inspectable.
        await self.client.create_stream(name=stream)
        for topic in ("requests", "responses"):
            await self.client.create_topic(
                stream=stream,
                name=topic,
                partitions_count=1,
                replication_factor=1,
            )

    async def send(self, stream: str, topic: str, payloads: list[bytes]) -> None:
        for index in range(0, len(payloads), 100):
            await self.client.send_messages(
                stream=stream,
                topic=topic,
                partitioning=0,
                messages=[
                    SendMessage(payload.decode())
                    for payload in payloads[index : index + 100]
                ],
            )

    async def read(
        self, stream: str, start: int, count: int
    ) -> list[tuple[int, bytes]]:
        result = []
        offset = start
        async with asyncio.timeout(30):
            while len(result) < count:
                messages = await self.client.poll_messages(
                    stream=stream,
                    topic="requests",
                    partition_id=0,
                    polling_strategy=PollingStrategy.Offset(offset),
                    count=min(100, count - len(result)),
                    auto_commit=False,
                )
                batch = list(messages)
                if not batch:
                    await asyncio.sleep(0.1)
                    continue
                for message in batch:
                    if message.offset() != offset:
                        raise RuntimeError(
                            f"Noncontiguous capture: expected {offset}, got {message.offset()}"
                        )
                    result.append((offset, bytes(message.payload())))
                    offset += 1
        return result
