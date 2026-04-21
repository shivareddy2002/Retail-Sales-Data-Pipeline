-- =============================================================================
-- FILE: 06_tasks/01_pipeline_tasks.sql
-- PROJECT: Retail Sales Data Pipeline
-- DESCRIPTION: Automate the ETL pipeline using Snowflake Tasks
--
-- SNOWFLAKE TASKS:
-- ─────────────────────────────────────────────────────────────────────────────
-- Tasks are scheduled SQL statements that run automatically.
-- They support:
--   - CRON schedules (like Linux cron)
--   - Minute-based intervals
--   - Conditional execution (WHEN clause)
--   - Task Trees (DAG of dependent tasks — Task A triggers Task B)
-- ─────────────────────────────────────────────────────────────────────────────

USE ROLE      RETAIL_ENGINEER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE  RETAIL_DB;

-- ----------------------------------------------------------------------------
-- Grant required privilege for tasks to execute
-- (EXECUTE TASK privilege is needed in production)
-- ----------------------------------------------------------------------------
-- GRANT EXECUTE TASK ON ACCOUNT TO ROLE RETAIL_ENGINEER_ROLE;

-- ============================================================================
-- TASK 1 (ROOT TASK): Check for new data and run incremental merge
-- This is the main workhorse task that runs every 5 minutes.
-- The WHEN clause means: "only run if the stream actually has new data"
-- → Avoids wasted compute when there's nothing to process
-- ============================================================================
CREATE OR REPLACE TASK RETAIL_DB.RAW.TASK_INCREMENTAL_MERGE
    WAREHOUSE   = RETAIL_WH
    SCHEDULE    = '5 MINUTE'    -- Run every 5 minutes
    WHEN
        -- Condition: only execute if the stream has unprocessed changes
        SYSTEM$STREAM_HAS_DATA('RETAIL_DB.RAW.SALES_CDC_STREAM')
    COMMENT = 'Incremental MERGE of CDC stream changes into STG_SALES every 5 minutes'
AS
    -- The MERGE statement from 05_merge_incremental/01_incremental_merge.sql
    -- (paste the full MERGE statement here in production)
    MERGE INTO RETAIL_DB.STAGING.STG_SALES AS target
    USING (
        SELECT
            TRIM(UPPER(ORDER_ID))                               AS ORDER_ID,
            TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD')               AS ORDER_DATE,
            YEAR(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))         AS ORDER_YEAR,
            MONTH(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))        AS ORDER_MONTH,
            MONTHNAME(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))    AS ORDER_MONTH_NAME,
            'Q' || QUARTER(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))
                || '-' || YEAR(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))
                                                                AS ORDER_QUARTER,
            DAYNAME(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))      AS ORDER_DAY_OF_WEEK,
            TRIM(COALESCE(CUSTOMER_ID, 'UNKNOWN'))              AS CUSTOMER_ID,
            INITCAP(TRIM(COALESCE(CUSTOMER_NAME, 'Unknown')))   AS CUSTOMER_NAME_CLEAN,
            COALESCE(TRIM(CUSTOMER_NAME), 'Unknown Customer')   AS CUSTOMER_NAME,
            TRIM(COALESCE(PRODUCT, 'Unknown Product'))          AS PRODUCT,
            TRIM(COALESCE(CATEGORY, 'Uncategorized'))           AS CATEGORY,
            UPPER(TRIM(COALESCE(CATEGORY, 'UNCATEGORIZED')))    AS CATEGORY_UPPER,
            INITCAP(TRIM(COALESCE(REGION, 'Unknown')))          AS REGION,
            COALESCE(TRY_TO_DOUBLE(SALES),    0.0)              AS SALES,
            COALESCE(TRY_TO_NUMBER(QUANTITY), 0)                AS QUANTITY,
            COALESCE(TRY_TO_DOUBLE(PROFIT),   0.0)              AS PROFIT,
            CASE WHEN COALESCE(TRY_TO_NUMBER(QUANTITY), 0) > 0
                 THEN ROUND(COALESCE(TRY_TO_DOUBLE(SALES), 0) /
                            COALESCE(TRY_TO_NUMBER(QUANTITY), 1), 2)
                 ELSE 0 END                                     AS UNIT_PRICE,
            CASE WHEN COALESCE(TRY_TO_DOUBLE(SALES), 0) > 0
                 THEN ROUND((COALESCE(TRY_TO_DOUBLE(PROFIT), 0) /
                             COALESCE(TRY_TO_DOUBLE(SALES),  1)) * 100, 2)
                 ELSE 0 END                                     AS PROFIT_RATIO,
            CASE WHEN COALESCE(TRY_TO_DOUBLE(SALES), 0) = 0 THEN 'No Revenue'
                 WHEN (COALESCE(TRY_TO_DOUBLE(PROFIT), 0) /
                       COALESCE(TRY_TO_DOUBLE(SALES), 1)) * 100 >= 30 THEN 'High'
                 WHEN (COALESCE(TRY_TO_DOUBLE(PROFIT), 0) /
                       COALESCE(TRY_TO_DOUBLE(SALES), 1)) * 100 >= 15 THEN 'Medium'
                 ELSE 'Low' END                                 AS PROFIT_MARGIN_BAND,
            CASE WHEN COALESCE(TRY_TO_DOUBLE(PROFIT), 0) > 0
                 THEN TRUE ELSE FALSE END                       AS IS_PROFITABLE,
            METADATA$ACTION                                     AS CDC_ACTION,
            METADATA$ISUPDATE                                   AS IS_UPDATE
        FROM RETAIL_DB.RAW.SALES_CDC_STREAM
        WHERE ORDER_ID IS NOT NULL
          AND TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD') IS NOT NULL
    ) AS source
    ON target.ORDER_ID = source.ORDER_ID
    WHEN MATCHED AND source.CDC_ACTION = 'INSERT' THEN
        UPDATE SET
            target.ORDER_DATE          = source.ORDER_DATE,
            target.SALES               = source.SALES,
            target.QUANTITY            = source.QUANTITY,
            target.PROFIT              = source.PROFIT,
            target.UNIT_PRICE          = source.UNIT_PRICE,
            target.PROFIT_RATIO        = source.PROFIT_RATIO,
            target.PROFIT_MARGIN_BAND  = source.PROFIT_MARGIN_BAND,
            target._LOAD_TIMESTAMP     = CURRENT_TIMESTAMP()
    WHEN MATCHED AND source.CDC_ACTION = 'DELETE' THEN DELETE
    WHEN NOT MATCHED AND source.CDC_ACTION = 'INSERT' THEN
        INSERT (ORDER_ID, ORDER_DATE, ORDER_YEAR, ORDER_MONTH, ORDER_MONTH_NAME,
                ORDER_QUARTER, ORDER_DAY_OF_WEEK, CUSTOMER_ID, CUSTOMER_NAME,
                CUSTOMER_NAME_CLEAN, PRODUCT, CATEGORY, CATEGORY_UPPER, REGION,
                SALES, QUANTITY, PROFIT, UNIT_PRICE, PROFIT_RATIO,
                PROFIT_MARGIN_BAND, IS_PROFITABLE, _SOURCE_SYSTEM)
        VALUES (source.ORDER_ID, source.ORDER_DATE, source.ORDER_YEAR,
                source.ORDER_MONTH, source.ORDER_MONTH_NAME, source.ORDER_QUARTER,
                source.ORDER_DAY_OF_WEEK, source.CUSTOMER_ID, source.CUSTOMER_NAME,
                source.CUSTOMER_NAME_CLEAN, source.PRODUCT, source.CATEGORY,
                source.CATEGORY_UPPER, source.REGION, source.SALES,
                source.QUANTITY, source.PROFIT, source.UNIT_PRICE,
                source.PROFIT_RATIO, source.PROFIT_MARGIN_BAND,
                source.IS_PROFITABLE, 'RETAIL_CSV');

-- ============================================================================
-- TASK 2 (CHILD TASK): Refresh Star Schema after staging is updated
-- This runs AFTER Task 1 completes using AFTER clause (Task Tree / DAG)
-- ============================================================================
CREATE OR REPLACE TASK RETAIL_DB.MART.TASK_REFRESH_STAR_SCHEMA
    WAREHOUSE = RETAIL_WH
    AFTER RETAIL_DB.RAW.TASK_INCREMENTAL_MERGE   -- Depends on Task 1
    COMMENT = 'Refresh all star schema dimension and fact tables after staging update'
AS
    -- Refresh fact table (uses stored procedure in production)
    -- See 07_star_schema for the full INSERT scripts
    INSERT INTO RETAIL_DB.MART.FACT_SALES
        SELECT
            s.ORDER_ID, dc.CUSTOMER_SK, dp.PRODUCT_SK,
            dr.REGION_SK, dd.DATE_SK,
            s.SALES, s.QUANTITY, s.PROFIT,
            s.UNIT_PRICE, s.PROFIT_RATIO, CURRENT_TIMESTAMP()
        FROM RETAIL_DB.STAGING.STG_SALES s
        LEFT JOIN RETAIL_DB.MART.DIM_CUSTOMER dc ON s.CUSTOMER_ID = dc.CUSTOMER_ID
        LEFT JOIN RETAIL_DB.MART.DIM_PRODUCT   dp ON s.PRODUCT = dp.PRODUCT_NAME
        LEFT JOIN RETAIL_DB.MART.DIM_REGION    dr ON s.REGION  = dr.REGION_NAME
        LEFT JOIN RETAIL_DB.MART.DIM_DATE      dd ON s.ORDER_DATE = dd.FULL_DATE
        WHERE s.ORDER_ID NOT IN (SELECT ORDER_ID FROM RETAIL_DB.MART.FACT_SALES);

-- ============================================================================
-- TASK LIFECYCLE MANAGEMENT
-- ============================================================================

-- Tasks are created in SUSPENDED state by default → manually RESUME to activate

-- Resume Root Task (this also activates child tasks in the tree)
ALTER TASK RETAIL_DB.RAW.TASK_INCREMENTAL_MERGE RESUME;

-- Resume Child Task
ALTER TASK RETAIL_DB.MART.TASK_REFRESH_STAR_SCHEMA RESUME;

-- Suspend all tasks (e.g., during maintenance)
-- ALTER TASK RETAIL_DB.MART.TASK_REFRESH_STAR_SCHEMA SUSPEND;
-- ALTER TASK RETAIL_DB.RAW.TASK_INCREMENTAL_MERGE SUSPEND;

-- ============================================================================
-- MONITOR TASK EXECUTION
-- ============================================================================

-- View all tasks in the database
SHOW TASKS IN DATABASE RETAIL_DB;

-- View task run history (last 10 runs)
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SECONDS,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
        RESULT_LIMIT               => 10
    )
)
ORDER BY SCHEDULED_TIME DESC;

-- ============================================================================
-- CRON SCHEDULE ALTERNATIVE (more flexible than minute-based)
-- Use this instead of '5 MINUTE' for business-hours only scheduling
-- ============================================================================
/*
CREATE OR REPLACE TASK RETAIL_DB.RAW.TASK_INCREMENTAL_MERGE_CRON
    WAREHOUSE = RETAIL_WH
    -- Run at 9am, 12pm, 3pm, 6pm on weekdays (UTC) — Mon-Fri
    SCHEDULE  = 'USING CRON 0 9,12,15,18 * * MON-FRI UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.RAW.SALES_CDC_STREAM')
AS
    -- ... same merge statement ...
*/
