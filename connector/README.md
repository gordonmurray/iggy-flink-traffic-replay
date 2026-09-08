# Vendored Iggy connector

Source: https://github.com/gordonmurray/flink-connector-iggy

Upstream commit: `7af974f` (version `0.2.0-SNAPSHOT`).

This copy makes a normal clone sufficient to build the example. No submodule or
separate connector checkout is required. The upstream MIT license is in LICENSE.

The example uses one partition and a fresh Flink job for each experiment.
Starting offsets apply to new splits. They do not override restored split offsets.
