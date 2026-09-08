package dev.iggy.demo;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/** Consumes measured HTTP responses through the Iggy SQL connector into Iceberg. */
public final class ReplayJob {
    public static void main(String[] args) {
        if (args.length != 1 || !args[0].matches("replay-[a-f0-9]{32}")) {
            throw new IllegalArgumentException("Expected an experiment stream: replay-<32 hex digits>");
        }
        var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        var config = table.getConfig().getConfiguration();
        config.setString("pipeline.name", "iggy-" + args[0]);
        config.setString("parallelism.default", "1");
        config.setString("execution.checkpointing.interval", "2 s");
        config.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        // Fail visibly. The demo does not claim validated connector recovery semantics.
        config.setString("restart-strategy.type", "none");

        table.executeSql("""
            CREATE CATALOG iceberg WITH (
              'type' = 'iceberg', 'catalog-type' = 'rest',
              'uri' = 'http://iceberg-rest:8181', 'warehouse' = 's3://warehouse/',
              'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
              's3.endpoint' = 'http://minio:9000', 's3.path-style-access' = 'true',
              's3.access-key-id' = 'demo-admin', 's3.secret-access-key' = 'demo-password'
            )
            """);
        table.executeSql("CREATE DATABASE IF NOT EXISTS iceberg.replay");
        table.executeSql("""
            CREATE TABLE IF NOT EXISTS iceberg.replay.traffic_runs (
              experiment_id STRING, run_id STRING, event_id STRING, sequence_no INT,
              request_offset BIGINT, request_sha256 STRING, scheduled_offset_ms DOUBLE,
              dispatch_lag_ms DOUBLE, endpoint STRING, `method` STRING,
              status_code INT, response_time_ms DOUBLE, response_size_bytes INT,
              `error` STRING, is_anomaly BOOLEAN
            ) PARTITIONED BY (experiment_id, run_id) WITH ('format-version' = '2')
            """);
        table.executeSql("""
            CREATE TEMPORARY TABLE responses (
              experiment_id STRING, run_id STRING, event_id STRING, sequence_no INT,
              request_offset BIGINT, request_sha256 STRING, scheduled_offset_ms DOUBLE,
              dispatch_lag_ms DOUBLE, endpoint STRING, `method` STRING,
              status_code INT, response_time_ms DOUBLE, response_size_bytes INT,
              `error` STRING
            ) WITH (
              'connector' = 'iggy', 'host' = 'iggy', 'stream' = '%s',
              'topic' = 'responses', 'format' = 'json', 'starting-offset' = 'earliest'
            )
            """.formatted(args[0]));
        table.executeSql("""
            INSERT INTO iceberg.replay.traffic_runs
            SELECT *, status_code >= 500 OR status_code = 0 OR response_time_ms >= 100.0
            FROM responses
            """);
    }
}
