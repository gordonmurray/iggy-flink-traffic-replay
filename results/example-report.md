# Replay results

Experiment: `601a6c3a057a401d97d458e18ceda552`

| Check | Result |
|---|---|
| all_responses_committed | PASS |
| no_duplicate_events | PASS |
| same_requests_and_offsets | PASS |
| no_null_measurements | PASS |
| no_transport_failures | PASS |
| original_pool_failure_observed | PASS |
| fixed_server_has_no_errors | PASS |
| anomalies_reduced_at_least_80_percent | PASS |
| dispatch_lag_below_250ms | PASS |
| flink_completed_checkpoint | PASS |
| flink_did_not_restart | PASS |

| Metric | Original | Replay |
|---|---:|---:|
| rows | 400 | 400 |
| unique_events | 400 | 400 |
| errors | 194 | 0 |
| anomalies | 194 | 0 |
| transport_errors | 0 | 0 |
| pool_timeouts | 194 | 0 |
| health_errors | 65 | 0 |
| latency_p95_ms | 155.515 | 31.988 |
| dispatch_lag_p95_ms | 1.521 | 1.778 |
| dispatch_lag_max_ms | 7.553 | 3.337 |
