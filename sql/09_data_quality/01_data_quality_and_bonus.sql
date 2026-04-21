-- =============================================================================
-- FILE: 09_data_quality/01_data_quality_checks.sql
-- PROJECT: Retail Sales Data Pipeline
-- DESCRIPTION: Data Quality Framework — validation checks, audit logging,
--              Snowflake Time Travel, and Query Optimization tips
-- =============================================================================

USE ROLE      RETAIL_ENGINEER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE  RETAIL_DB;

-- ============================================================================
-- BONUS FEATURE 1: DATA QUALITY FRAMEWORK
-- ============================================================================

-- Create an audit table to log DQ check results over time
CREATE OR REPLACE TABLE RETAIL_DB.AUDIT.DQ_CHECK_LOG (
    CHECK_ID        NUMBER AUTOINCREMENT,
    CHECK_NAME      VARCHAR(100)    NOT NULL    COMMENT 'Name of the DQ check',
    TABLE_NAME      VARCHAR(200)    NOT NULL    COMMENT 'Table being checked',
    CHECK_QUERY     VARCHAR(2000)               COMMENT 'The SQL check that was run',
    STATUS          VARCHAR(10)                 COMMENT 'PASS or FAIL',
    ROWS_CHECKED    NUMBER                      COMMENT 'Total rows in scope',
    ROWS_FAILED     NUMBER                      COMMENT 'Rows that violated the rule',
    FAILURE_RATE    FLOAT                       COMMENT 'rows_failed / rows_checked',
    THRESHOLD       FLOAT                       COMMENT 'Max acceptable failure rate',
    RUN_TIMESTAMP   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMMENTS        VARCHAR(500),
    CONSTRAINT PK_DQ_LOG PRIMARY KEY (CHECK_ID)
);

-- ----------------------------------------------------------------------------
-- DQ CHECK PROCEDURE: Run all checks and log results
-- ----------------------------------------------------------------------------

-- CHECK 1: Completeness — no NULL ORDER_IDs
INSERT INTO RETAIL_DB.AUDIT.DQ_CHECK_LOG
    (CHECK_NAME, TABLE_NAME, STATUS, ROWS_CHECKED, ROWS_FAILED, FAILURE_RATE, THRESHOLD, COMMENTS)
SELECT
    'NULL_ORDER_ID_CHECK'                       AS CHECK_NAME,
    'RETAIL_DB.STAGING.STG_SALES'               AS TABLE_NAME,
    CASE WHEN null_count = 0 THEN 'PASS' ELSE 'FAIL' END AS STATUS,
    total_rows,
    null_count,
    ROUND(null_count / NULLIF(total_rows, 0), 4) AS FAILURE_RATE,
    0.00                                        AS THRESHOLD,  -- 0% allowed
    'Order ID must never be NULL'               AS COMMENTS
FROM (
    SELECT
        COUNT(*)                                         AS total_rows,
        SUM(CASE WHEN ORDER_ID IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM RETAIL_DB.STAGING.STG_SALES
);

-- CHECK 2: Validity — SALES must be positive
INSERT INTO RETAIL_DB.AUDIT.DQ_CHECK_LOG
    (CHECK_NAME, TABLE_NAME, STATUS, ROWS_CHECKED, ROWS_FAILED, FAILURE_RATE, THRESHOLD, COMMENTS)
SELECT
    'NEGATIVE_SALES_CHECK',
    'RETAIL_DB.STAGING.STG_SALES',
    CASE WHEN neg_sales = 0 THEN 'PASS' ELSE 'FAIL' END,
    total_rows,
    neg_sales,
    ROUND(neg_sales / NULLIF(total_rows, 0), 4),
    0.00,
    'Sales amount must be >= 0'
FROM (
    SELECT
        COUNT(*)                                          AS total_rows,
        SUM(CASE WHEN SALES < 0 THEN 1 ELSE 0 END)       AS neg_sales
    FROM RETAIL_DB.STAGING.STG_SALES
);

-- CHECK 3: Uniqueness — ORDER_ID must be unique
INSERT INTO RETAIL_DB.AUDIT.DQ_CHECK_LOG
    (CHECK_NAME, TABLE_NAME, STATUS, ROWS_CHECKED, ROWS_FAILED, FAILURE_RATE, THRESHOLD, COMMENTS)
SELECT
    'DUPLICATE_ORDER_ID_CHECK',
    'RETAIL_DB.STAGING.STG_SALES',
    CASE WHEN dup_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    total_rows,
    dup_count,
    ROUND(dup_count / NULLIF(total_rows, 0), 4),
    0.00,
    'ORDER_ID must be unique — no duplicate orders allowed'
FROM (
    SELECT
        COUNT(*)                                          AS total_rows,
        COUNT(*) - COUNT(DISTINCT ORDER_ID)               AS dup_count
    FROM RETAIL_DB.STAGING.STG_SALES
);

-- CHECK 4: Referential Integrity — all fact orders have dimension matches
INSERT INTO RETAIL_DB.AUDIT.DQ_CHECK_LOG
    (CHECK_NAME, TABLE_NAME, STATUS, ROWS_CHECKED, ROWS_FAILED, FAILURE_RATE, THRESHOLD, COMMENTS)
SELECT
    'ORPHANED_FACT_ROWS_CHECK',
    'RETAIL_DB.MART.FACT_SALES',
    CASE WHEN orphans = 0 THEN 'PASS' ELSE 'FAIL' END,
    total_rows,
    orphans,
    ROUND(orphans / NULLIF(total_rows, 0), 4),
    0.01,  -- Allow up to 1% for edge cases
    'FACT_SALES rows must have matching DIM_CUSTOMER and DIM_PRODUCT'
FROM (
    SELECT
        COUNT(*)                                          AS total_rows,
        SUM(CASE WHEN CUSTOMER_SK IS NULL
                   OR PRODUCT_SK  IS NULL THEN 1 ELSE 0 END) AS orphans
    FROM RETAIL_DB.MART.FACT_SALES
);

-- CHECK 5: Timeliness — ensure data is not more than 2 days old
INSERT INTO RETAIL_DB.AUDIT.DQ_CHECK_LOG
    (CHECK_NAME, TABLE_NAME, STATUS, ROWS_CHECKED, ROWS_FAILED, FAILURE_RATE, THRESHOLD, COMMENTS)
SELECT
    'DATA_FRESHNESS_CHECK',
    'RETAIL_DB.RAW.RAW_SALES',
    CASE WHEN max_load_age_hours <= 48 THEN 'PASS' ELSE 'FAIL' END,
    1 AS ROWS_CHECKED,
    0 AS ROWS_FAILED,
    0.0,
    0.0,
    'Last load was ' || max_load_age_hours || ' hours ago'
FROM (
    SELECT DATEDIFF('hour', MAX(_LOAD_TIMESTAMP), CURRENT_TIMESTAMP()) AS max_load_age_hours
    FROM RETAIL_DB.RAW.RAW_SALES
);

-- View DQ check summary
SELECT
    CHECK_NAME,
    TABLE_NAME,
    STATUS,
    ROWS_CHECKED,
    ROWS_FAILED,
    ROUND(FAILURE_RATE * 100, 2) AS FAILURE_PCT,
    RUN_TIMESTAMP
FROM RETAIL_DB.AUDIT.DQ_CHECK_LOG
ORDER BY RUN_TIMESTAMP DESC;

-- ============================================================================
-- BONUS FEATURE 2: TIME TRAVEL
-- Snowflake retains historical data for up to 90 days (Enterprise) / 1 day (Standard)
-- Use cases: disaster recovery, audit trail, debugging, comparing snapshots
-- ============================================================================

-- Scenario: "What did the table look like 1 hour ago?"
SELECT COUNT(*) AS row_count_1_hour_ago
FROM RETAIL_DB.STAGING.STG_SALES
    AT (OFFSET => -3600);  -- 3600 seconds = 1 hour ago

-- Scenario: "Restore data to a specific timestamp (before accidental delete)"
-- CREATE OR REPLACE TABLE RETAIL_DB.STAGING.STG_SALES_RESTORED AS
--     SELECT * FROM RETAIL_DB.STAGING.STG_SALES
--     AT (TIMESTAMP => '2024-06-01 10:00:00'::TIMESTAMP_NTZ);

-- Scenario: "What changed in the last hour?"
SELECT
    new_data.ORDER_ID,
    old_data.SALES AS SALES_BEFORE,
    new_data.SALES AS SALES_AFTER,
    new_data.SALES - old_data.SALES AS DIFFERENCE
FROM RETAIL_DB.STAGING.STG_SALES AS new_data
JOIN RETAIL_DB.STAGING.STG_SALES AT (OFFSET => -3600) AS old_data
    ON new_data.ORDER_ID = old_data.ORDER_ID
WHERE new_data.SALES != old_data.SALES;

-- ============================================================================
-- BONUS FEATURE 3: QUERY OPTIMIZATION TIPS
-- ============================================================================

-- TIP 1: Add CLUSTER KEY on the fact table for large datasets
-- Micro-partitioning by date speeds up date-range queries significantly
ALTER TABLE RETAIL_DB.MART.FACT_SALES CLUSTER BY (DATE_SK);

-- TIP 2: Use RESULT_CACHE — Snowflake caches identical query results for 24h
-- No code needed — it's automatic. Just be aware of it.
-- SELECT ... -- If same query runs again within 24h, returns instantly from cache

-- TIP 3: SEARCH OPTIMIZATION SERVICE — for point lookup queries
-- ALTER TABLE RETAIL_DB.MART.FACT_SALES ADD SEARCH OPTIMIZATION ON EQUALITY(ORDER_ID);

-- TIP 4: EXPLAIN PLAN — inspect query execution before running expensive queries
EXPLAIN USING TABULAR
SELECT
    dp.CATEGORY,
    SUM(fs.SALES_AMOUNT) AS REVENUE
FROM RETAIL_DB.MART.FACT_SALES fs
JOIN RETAIL_DB.MART.DIM_PRODUCT dp ON fs.PRODUCT_SK = dp.PRODUCT_SK
GROUP BY dp.CATEGORY;

-- TIP 5: WAREHOUSE SCALING
-- For heavy BI loads or concurrent users, scale up temporarily:
-- ALTER WAREHOUSE RETAIL_WH SET WAREHOUSE_SIZE = 'LARGE';
-- ... run your heavy workload ...
-- ALTER WAREHOUSE RETAIL_WH SET WAREHOUSE_SIZE = 'X-SMALL';  -- scale back down

-- TIP 6: MULTI-CLUSTER WAREHOUSE for concurrency
-- When 10+ users run queries simultaneously:
-- ALTER WAREHOUSE RETAIL_WH SET
--     MIN_CLUSTER_COUNT = 1
--     MAX_CLUSTER_COUNT = 3
--     SCALING_POLICY    = 'ECONOMY';

-- ============================================================================
-- VIEW the final DQ Dashboard
-- ============================================================================
SELECT
    STATUS,
    COUNT(*) AS CHECK_COUNT,
    LISTAGG(CHECK_NAME, ', ') WITHIN GROUP (ORDER BY CHECK_NAME) AS CHECKS
FROM RETAIL_DB.AUDIT.DQ_CHECK_LOG
WHERE DATE(RUN_TIMESTAMP) = CURRENT_DATE()
GROUP BY STATUS;
