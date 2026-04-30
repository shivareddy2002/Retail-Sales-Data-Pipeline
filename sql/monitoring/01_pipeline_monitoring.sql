USE DATABASE RETAIL_DATA_PLATFORM;

CREATE OR REPLACE TABLE MONITORING.PIPELINE_RUN_LOG (
  run_id NUMBER AUTOINCREMENT,
  pipeline_name STRING,
  task_name STRING,
  status STRING,
  rows_processed NUMBER,
  started_at TIMESTAMP_NTZ,
  ended_at TIMESTAMP_NTZ,
  duration_seconds NUMBER,
  query_id STRING
);

CREATE OR REPLACE TABLE MONITORING.ERROR_TRACKING (
  error_id NUMBER AUTOINCREMENT,
  pipeline_name STRING,
  task_name STRING,
  error_code STRING,
  error_message STRING,
  occurred_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Query performance tracking view
CREATE OR REPLACE VIEW MONITORING.VW_QUERY_PERF AS
SELECT query_id, user_name, warehouse_name, total_elapsed_time/1000 AS elapsed_sec,
       bytes_scanned, rows_produced, start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name='RETAIL_DATA_PLATFORM'
  AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP());
