# Flink + Iggy traffic replay

**Would your fix survive the same requests?**

This example captures HTTP request inputs in Apache Iggy, then sends them to two versions of a local web server.
The first version leaks connection-pool slots. The second version releases them.
Apache Flink reads the measured responses from Iggy and writes them to Apache Iceberg on MinIO.
A DuckDB report compares the committed results and returns a nonzero exit code if any acceptance check fails.

## Run the example

Requirements:

- Docker Engine or Docker Desktop with Docker Compose v2 and BuildKit.
- GNU Make, or the equivalent Compose commands below.
- About 6 GB of memory available to Docker and 8 GB of free disk space for the build.
- Internet access for the first build. Local verification used Linux x86_64.

Run:

```bash
git clone https://github.com/gordonmurray/iggy-flink-traffic-replay.git
cd iggy-flink-traffic-replay
make run
```

No local Java, Maven, Python, cloud account, or connector checkout is required.
The first build downloads dependencies. Each default experiment then sends two sets of 400 requests at 40 requests per second.
Flink startup and Iceberg commits add time to the approximately 20-second HTTP workload.

Without Make:

```bash
docker compose build flink-jobmanager demo web-v1 web-v2 iceberg-rest
docker compose up -d --wait iggy web-v1 web-v2 minio iceberg-rest flink-jobmanager flink-taskmanager
docker compose run --rm --no-deps demo
```

For direct Compose use on Linux, set `LOCAL_UID` and `LOCAL_GID` if your user IDs differ from 1000.
Make sets these values automatically.

The report appears in the terminal and in `results/report.md` and `results/report.json`.
The Flink dashboard is at http://localhost:8081. The runner cancels its Flink job after the report.
The completed experiment therefore appears as a canceled job after a successful run.

## Data flow

```text
Python runner
  | generate request inputs once
  v
Iggy: requests (one partition, saved offset range)
  |                              |
  | original: read saved range   | replay: read the same range
  v                              v
HTTP server v1                  HTTP server v2
  | leaks pool slots              | releases pool slots
  +---------------+---------------+
                  | measured responses with request IDs and hashes
                  v
            Iggy: responses
                  |
                  v
       Custom Iggy source connector
                  |
                  v
       Flink SQL: classify failures
                  |
                  v
      Iceberg traffic_runs on MinIO
                  |
                  v
      PyIceberg scan -> DuckDB report
```

The report reads Iceberg data files through PyIceberg. It does not use a local copy of the HTTP responses as evidence.
Flink marks a response as anomalous when its status is at least 500, its request fails, or its latency reaches 100 ms.
This threshold fits the simulated 10–29 ms work and 150 ms pool timeout.
It is a rule for this example, not a statistical detector.

## What the report checks

- Exactly 400 committed rows per run with no duplicate event IDs.
- Identical request hashes, offsets, IDs, methods, endpoints, and planned dispatch times across both runs.
- No missing measurements or HTTP transport failures.
- At least ten pool timeouts in the original run, including failed health requests.
- No HTTP errors in the replay run.
- At least an 80% reduction in anomalies.
- A maximum dispatch delay below 250 ms in both runs.
- At least one completed Flink checkpoint and no Flink restarts during the experiment.

A real local run produced the following results. Timing varies with the host.
The complete measured output is in [results/example-report.md](results/example-report.md).

| Metric | Original | Replay |
|---|---:|---:|
| Committed responses | 400 | 400 |
| HTTP errors | 194 | 0 |
| Failed health requests | 65 | 0 |
| p95 response latency | 155.339 ms | 32.438 ms |

## Repeat runs and retained data

Each experiment creates a unique Iggy stream and uses a new Flink job.
Both request and response topics have one partition.
The runner records the stream, offset range, payload hash, configuration, and Flink job ID in `results/manifest.json`.
It reads the same retained request range before each HTTP run. It never regenerates the replay inputs.

Each experiment resets both local server pools. Concurrent runners that share `results/` are rejected.
Iceberg partitions results by experiment ID and run ID, so earlier runs cannot satisfy a later run's checks.
Iggy data, Iceberg files, and the SQLite REST catalog persist in named Docker volumes.
The files in `results/` describe the latest invocation.

To check the retained Iggy requests and read the last experiment from Iceberg again:

```bash
make report
```

To stop the services and retain their data:

```bash
make down
```

To start the services again, use `make run` for a new experiment.
To inspect an earlier retained report after a stop, start the services before `make report`:

```bash
docker compose up -d --wait iggy web-v1 web-v2 minio iceberg-rest flink-jobmanager flink-taskmanager
make report
```

**`make clean` deletes this example's named volumes and all captured broker and Iceberg data.**
It leaves the local report files intact.

## Configuration

Defaults require no `.env` file. Optional overrides are listed in `.env.example`.

| Variable | Default | Meaning |
|---|---:|---|
| `REQUEST_COUNT` | `400` | Requests in each run, between 100 and 5000 |
| `TRAFFIC_RATE_RPS` | `40` | Planned requests per second, between 1 and 100 |
| `TRAFFIC_SEED` | `42` | Seed for the request mix |
| `FLINK_UI_PORT` | `8081` | Local Flink dashboard port |

For example:

```bash
REQUEST_COUNT=600 TRAFFIC_RATE_RPS=60 make run
```

The fixed acceptance thresholds apply to every configuration. An overloaded host or a different workload can fail them.
Only the Flink dashboard is published to the host, bound to loopback.
The broker, object store, catalog, and test HTTP servers stay on the Compose network.
Credentials are public demo values. The example is intended for local use.

## Scope and limitations

This example replays the same request inputs at the same **planned** intervals.
It measures actual dispatch delay and new HTTP responses.
Concurrent completion order and exact wall-clock timing can differ.
The report does not claim deterministic operating-system scheduling or identical server state.

Flink checkpoints commit the Iceberg sink. The example does not restore a Flink savepoint or rewind model state.
The connector's starting-offset policy only applies to new splits, not checkpoint-restored splits.
A fresh job avoids conflating source recovery with replay of HTTP side effects.
End-to-end exactly-once behavior under process or network failure is not established here.

The workload contains methods, paths, and a correlation header. It has no request bodies, authentication, or external side effects.
The pool leak is deliberate and reproducible. Every second checkout leaks a slot in v1.
Both versions perform the same simulated work for a given request ID.

Z-score, River, SQL pattern detection, savepoint restoration, and broker benchmarks are possible extensions.
They are not implemented features of this example.

## Development

```bash
make test
```

The image build runs the connector's Java unit tests. `make test` runs the Python acceptance-check and HTTP-execution tests.
Broker-dependent upstream tests are opt-in and do not run during the image build.
`make run` exercises the actual broker, both HTTP servers, the connector, Flink, Iceberg, and the report.

The GitHub Actions workflow runs the unit tests, the full example twice, and a report after a service restart.

| Path | Purpose |
|---|---|
| `producer/` | Capture, HTTP execution, Flink submission, and result verification |
| `web-server/` | Shared server implementation with the v1 leak flag |
| `connector/` | Vendored connector source and upstream provenance |
| `flink-job/` | Java SQL job and multi-stage build |
| `iceberg/` | REST catalog image with a writable persistent directory |
| `tests/` | Tests that reject incomplete or incorrect experiments |
| `docs/design.md` | Design decisions and findings from the original prototype |

The connector is copied from [flink-connector-iggy](https://github.com/gordonmurray/flink-connector-iggy) at commit `7af974f`.
The example pins Iggy and its SDK to 0.7.0, Flink to 1.20.3, and the Iceberg Flink runtime to 1.7.1.
These are the versions used for local verification. They are not claims about the latest upstream releases.

## Troubleshooting

Use `make logs` for service logs. Use `make ps` for container status.
A failed Flink job also appears in the dashboard, with its exception.

- If the dashboard port is occupied, set `FLINK_UI_PORT` to a free port.
- If the report shows dispatch delays, reduce other host workloads and repeat the run.
- If Iceberg rows are missing, inspect the Flink job and checkpoint errors before another run.
- If `make report` follows `make clean`, its data no longer exists. Run `make run` to create a new experiment.
