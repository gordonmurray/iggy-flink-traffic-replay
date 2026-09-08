# Design and prototype review

The example was revised and checked on 2026-09-08.

## Capture inputs and measure new outputs

The original producer generated synthetic latency and status values. It did not call either HTTP server.
Replaying those response records could repeat analytics, but could not establish whether a server fix worked.

The example now stores request inputs in Iggy before execution.
Both runs consume the saved offset range and measure new HTTP responses.
Every response carries its request ID, offset, and SHA-256 hash.
Flink processes both runs through the same SQL pipeline.
The report compares committed Iceberg rows by request ID.

## Define the replay boundary

The original plan combined a savepoint restore with a new starting offset.
The connector intentionally preserves restored split offsets, so that configuration does not rewind restored source state.

This example keeps a single Flink job active while both HTTP runs execute.
The runner performs the request seek independently, with explicit offsets and `auto_commit=False`.
A new experiment uses a unique stream and a fresh Flink job.
This isolates experiments and avoids duplicate HTTP side effects from a Flink restart.
The job uses no automatic restart strategy. Failures stop the experiment.

Checkpoint recovery, rescaling, and transactional HTTP effects need separate designs and tests.
The vendored connector also retains upstream limitations: synchronous poll waits, outstanding timed-out polls, and incomplete SDK connection cleanup.
The finite demo does not establish suitability for a long-running production deployment.

## Simplify Flink deployment

The old PyFlink pipeline failed to load JSON and Iceberg classes.
The Dockerfile downloaded runtime JARs with remote `ADD` instructions and no permission override.
Docker assigns those files mode `0600` by default, while Flink runs as a non-root user.
See the [Dockerfile reference](https://docs.docker.com/reference/dockerfile/#adding-files-from-a-url).

The new image explicitly uses mode `0644` for dependency JARs.
A small Java SQL job removes the Python runtime and duplicate user-library layout from the Flink containers.
The successful end-to-end run verifies this deployment.
It does not prove that file permissions were the only cause of the old failure.

## Keep results durable and independent

MinIO stores the Iceberg files. A SQLite database on a named volume stores the REST catalog's table metadata.
Both are necessary to find the tables after a container replacement.

The broker stores data under `/app/local_data`. Its volume must mount at that path.
The original `/local_data` mount left request data in the container layer.
A container-replacement test exposed this data loss.
The report now checks the retained request hash as well as the Iceberg rows.
The catalog image creates its data directory with the service user's ownership.

The runner waits for committed rows and reads them through PyIceberg.
DuckDB checks row completeness, duplicate IDs, request equality, errors, anomalies, and dispatch delays.
The report also records completed Flink checkpoints and restart count.
A transport failure cannot count as evidence of the intended pool leak.
An HTTP client error in the fixed server cannot pass as success.

## Keep the example bounded

The original plan included three detectors, state restoration, a comparison report, and a broker benchmark.
The example now has one rule-based detector and one complete request-replay loop.
The remaining ideas are extensions, not advertised behavior.

The connector source is vendored with its MIT license and upstream commit.
A normal clone therefore contains all project source required for the build.
Container builds still download pinned dependencies and base images.

## Local verification

The final checks ran on Linux x86_64 with Docker on 2026-09-08.
Forty Java unit tests and 23 Python tests passed. Eight broker-dependent upstream Java tests were skipped during the build.
The full Compose example passed with 400 requests at 40 requests per second and 600 requests at 60 requests per second.
A clean source copy required no submodule checkout or local configuration file.
After container replacement, both the retained Iggy request hash and the committed Iceberg rows matched.
A concurrent runner was rejected. SIGTERM marked the interrupted run as failed and canceled its Flink job.
The example report contains output from an actual run in the working checkout.
