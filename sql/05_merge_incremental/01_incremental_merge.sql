-- =============================================================================
-- FILE: 05_merge_incremental/01_incremental_merge.sql
-- PROJECT: Retail Sales Data Pipeline
-- DESCRIPTION: Incremental load using MERGE + CDC Stream
--              Handles INSERT, UPDATE, and DELETE scenarios
--
-- INCREMENTAL LOADING STRATEGY:
-- ─────────────────────────────────────────────────────────────────────────────
-- Instead of truncating and reloading the entire staging table every run
-- (expensive for large datasets), we use:
--   1. Stream → captures only CHANGED rows since last run
--   2. MERGE  → applies those changes to the target table efficiently
--
-- This is called SCD Type 1 (Slowly Changing Dimension Type 1):
--   → Simply overwrite the existing record with the latest values
-- ─────────────────────────────────────────────────────────────────────────────

USE ROLE      RETAIL_ENGINEER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE  RETAIL_DB;

-- ----------------------------------------------------------------------------
-- PROCEDURE: MERGE changed rows from stream into STG_SALES
-- This is the core of our incremental pipeline
-- ----------------------------------------------------------------------------

-- First, check if there's anything to process
SELECT SYSTEM$STREAM_HAS_DATA('RETAIL_DB.RAW.SALES_CDC_STREAM') AS HAS_DATA;

-- ----------------------------------------------------------------------------
-- THE MERGE STATEMENT
-- Source: CDC stream (only changed rows since last run)
-- Target: STG_SALES (the clean staging table)
--
-- Logic:
--   MATCHED + DELETE action   → DELETE from target (or mark as deleted)
--   MATCHED + INSERT action   → UPDATE the target row with new values
--   NOT MATCHED + INSERT      → INSERT the new row into target
-- ----------------------------------------------------------------------------
MERGE INTO RETAIL_DB.STAGING.STG_SALES AS target
USING (
    -- ─────────────────────────────────────────────────────────────────────
    -- SOURCE: Read from CDC stream and apply the same transformations
    -- as the initial load, so the staging table always has clean data.
    -- ─────────────────────────────────────────────────────────────────────
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
        COALESCE(TRIM(CUSTOMER_NAME), 'Unknown Customer')   AS CUSTOMER_NAME,
        INITCAP(TRIM(COALESCE(CUSTOMER_NAME, 'Unknown')))   AS CUSTOMER_NAME_CLEAN,
        TRIM(COALESCE(PRODUCT,  'Unknown Product'))         AS PRODUCT,
        TRIM(COALESCE(CATEGORY, 'Uncategorized'))           AS CATEGORY,
        UPPER(TRIM(COALESCE(CATEGORY, 'UNCATEGORIZED')))    AS CATEGORY_UPPER,
        INITCAP(TRIM(COALESCE(REGION, 'Unknown')))          AS REGION,
        COALESCE(TRY_TO_DOUBLE(SALES),    0.0)              AS SALES,
        COALESCE(TRY_TO_NUMBER(QUANTITY), 0)                AS QUANTITY,
        COALESCE(TRY_TO_DOUBLE(PROFIT),   0.0)              AS PROFIT,
        CASE
            WHEN COALESCE(TRY_TO_NUMBER(QUANTITY), 0) > 0
            THEN ROUND(COALESCE(TRY_TO_DOUBLE(SALES), 0) /
                       COALESCE(TRY_TO_NUMBER(QUANTITY), 1), 2)
            ELSE 0
        END                                                 AS UNIT_PRICE,
        CASE
            WHEN COALESCE(TRY_TO_DOUBLE(SALES), 0) > 0
            THEN ROUND((COALESCE(TRY_TO_DOUBLE(PROFIT), 0) /
                        COALESCE(TRY_TO_DOUBLE(SALES),  1)) * 100, 2)
            ELSE 0
        END                                                 AS PROFIT_RATIO,
        CASE
            WHEN COALESCE(TRY_TO_DOUBLE(SALES), 0) = 0 THEN 'No Revenue'
            WHEN (COALESCE(TRY_TO_DOUBLE(PROFIT), 0) /
                  COALESCE(TRY_TO_DOUBLE(SALES), 1)) * 100 >= 30 THEN 'High'
            WHEN (COALESCE(TRY_TO_DOUBLE(PROFIT), 0) /
                  COALESCE(TRY_TO_DOUBLE(SALES), 1)) * 100 >= 15 THEN 'Medium'
            ELSE 'Low'
        END                                                 AS PROFIT_MARGIN_BAND,
        CASE
            WHEN COALESCE(TRY_TO_DOUBLE(PROFIT), 0) > 0 THEN TRUE
            ELSE FALSE
        END                                                 AS IS_PROFITABLE,
        -- Stream CDC metadata (needed to decide INSERT vs DELETE)
        METADATA$ACTION                                     AS CDC_ACTION,
        METADATA$ISUPDATE                                   AS IS_UPDATE
    FROM RETAIL_DB.RAW.SALES_CDC_STREAM
    WHERE
        ORDER_ID IS NOT NULL
        AND TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD') IS NOT NULL
) AS source

-- Join on the business key
ON target.ORDER_ID = source.ORDER_ID

-- ─────────────────────────────────────────────────────────────────────────────
-- CASE 1: RECORD EXISTS IN TARGET AND SOURCE IS AN UPDATE (INSERT action of the pair)
-- → Overwrite the target row with the fresh values from the stream
-- ─────────────────────────────────────────────────────────────────────────────
WHEN MATCHED AND source.CDC_ACTION = 'INSERT' THEN
    UPDATE SET
        target.ORDER_DATE           = source.ORDER_DATE,
        target.ORDER_YEAR           = source.ORDER_YEAR,
        target.ORDER_MONTH          = source.ORDER_MONTH,
        target.ORDER_MONTH_NAME     = source.ORDER_MONTH_NAME,
        target.ORDER_QUARTER        = source.ORDER_QUARTER,
        target.ORDER_DAY_OF_WEEK    = source.ORDER_DAY_OF_WEEK,
        target.CUSTOMER_ID          = source.CUSTOMER_ID,
        target.CUSTOMER_NAME        = source.CUSTOMER_NAME,
        target.CUSTOMER_NAME_CLEAN  = source.CUSTOMER_NAME_CLEAN,
        target.PRODUCT              = source.PRODUCT,
        target.CATEGORY             = source.CATEGORY,
        target.CATEGORY_UPPER       = source.CATEGORY_UPPER,
        target.REGION               = source.REGION,
        target.SALES                = source.SALES,
        target.QUANTITY             = source.QUANTITY,
        target.PROFIT               = source.PROFIT,
        target.UNIT_PRICE           = source.UNIT_PRICE,
        target.PROFIT_RATIO         = source.PROFIT_RATIO,
        target.PROFIT_MARGIN_BAND   = source.PROFIT_MARGIN_BAND,
        target.IS_PROFITABLE        = source.IS_PROFITABLE,
        target._LOAD_TIMESTAMP      = CURRENT_TIMESTAMP()

-- ─────────────────────────────────────────────────────────────────────────────
-- CASE 2: RECORD EXISTS IN TARGET AND SOURCE ACTION IS DELETE
-- → Remove the record from the staging table
-- ─────────────────────────────────────────────────────────────────────────────
WHEN MATCHED AND source.CDC_ACTION = 'DELETE' THEN
    DELETE

-- ─────────────────────────────────────────────────────────────────────────────
-- CASE 3: NEW RECORD (not in target yet)
-- → Insert the transformed row into the staging table
-- ─────────────────────────────────────────────────────────────────────────────
WHEN NOT MATCHED AND source.CDC_ACTION = 'INSERT' THEN
    INSERT (
        ORDER_ID, ORDER_DATE, ORDER_YEAR, ORDER_MONTH, ORDER_MONTH_NAME,
        ORDER_QUARTER, ORDER_DAY_OF_WEEK, CUSTOMER_ID, CUSTOMER_NAME,
        CUSTOMER_NAME_CLEAN, PRODUCT, CATEGORY, CATEGORY_UPPER, REGION,
        SALES, QUANTITY, PROFIT, UNIT_PRICE, PROFIT_RATIO,
        PROFIT_MARGIN_BAND, IS_PROFITABLE, _SOURCE_SYSTEM, _TRANSFORM_VERSION
    )
    VALUES (
        source.ORDER_ID, source.ORDER_DATE, source.ORDER_YEAR, source.ORDER_MONTH,
        source.ORDER_MONTH_NAME, source.ORDER_QUARTER, source.ORDER_DAY_OF_WEEK,
        source.CUSTOMER_ID, source.CUSTOMER_NAME, source.CUSTOMER_NAME_CLEAN,
        source.PRODUCT, source.CATEGORY, source.CATEGORY_UPPER, source.REGION,
        source.SALES, source.QUANTITY, source.PROFIT, source.UNIT_PRICE,
        source.PROFIT_RATIO, source.PROFIT_MARGIN_BAND, source.IS_PROFITABLE,
        'RETAIL_CSV', 'v1.0'
    );

-- ----------------------------------------------------------------------------
-- Verify results after merge
-- ----------------------------------------------------------------------------
SELECT COUNT(*) AS total_rows_after_merge FROM RETAIL_DB.STAGING.STG_SALES;

-- Confirm stream offset was advanced (should now be empty)
SELECT SYSTEM$STREAM_HAS_DATA('RETAIL_DB.RAW.SALES_CDC_STREAM') AS STILL_HAS_DATA;
-- Expected: FALSE — stream was consumed by the MERGE above
