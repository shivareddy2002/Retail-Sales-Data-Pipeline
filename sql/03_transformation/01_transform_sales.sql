-- =============================================================================
-- FILE: 03_transformation/01_transform_sales.sql
-- PROJECT: Retail Sales Data Pipeline
-- DESCRIPTION: Transform raw data → clean, typed, enriched staging table
--              Handles NULLs, casts data types, adds derived columns
-- =============================================================================

USE ROLE      RETAIL_ENGINEER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE  RETAIL_DB;
USE SCHEMA    RETAIL_DB.STAGING;

-- ----------------------------------------------------------------------------
-- STEP 1: Create the Transformed / Cleaned Sales Table
-- PURPOSE: Production-grade table with correct data types and business logic
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RETAIL_DB.STAGING.STG_SALES (
    -- Primary key
    ORDER_ID            VARCHAR(20)     NOT NULL,

    -- Date fields
    ORDER_DATE          DATE            NOT NULL,
    ORDER_YEAR          NUMBER(4)                COMMENT 'Extracted year from order date',
    ORDER_MONTH         NUMBER(2)                COMMENT 'Extracted month number (1-12)',
    ORDER_MONTH_NAME    VARCHAR(20)              COMMENT 'Month name e.g. January',
    ORDER_QUARTER       VARCHAR(6)               COMMENT 'Quarter label e.g. Q1-2024',
    ORDER_DAY_OF_WEEK   VARCHAR(15)              COMMENT 'Day name e.g. Monday',

    -- Customer fields
    CUSTOMER_ID         VARCHAR(20)     NOT NULL,
    CUSTOMER_NAME       VARCHAR(100),
    CUSTOMER_NAME_CLEAN VARCHAR(100)             COMMENT 'Trimmed & title-cased customer name',

    -- Product fields
    PRODUCT             VARCHAR(200),
    CATEGORY            VARCHAR(100),
    CATEGORY_UPPER      VARCHAR(100)             COMMENT 'Standardized uppercase category',

    -- Geography
    REGION              VARCHAR(50),

    -- Financial metrics (properly typed)
    SALES               FLOAT           NOT NULL,
    QUANTITY            INT             NOT NULL,
    PROFIT              FLOAT           NOT NULL,

    -- Derived business metrics
    UNIT_PRICE          FLOAT                    COMMENT 'Sales / Quantity',
    PROFIT_RATIO        FLOAT                    COMMENT 'Profit / Sales * 100 (as percentage)',
    PROFIT_MARGIN_BAND  VARCHAR(20)              COMMENT 'High/Medium/Low based on profit ratio',
    IS_PROFITABLE       BOOLEAN                  COMMENT 'TRUE if profit > 0',

    -- Audit columns
    _LOAD_TIMESTAMP     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_SYSTEM      VARCHAR(50)     DEFAULT 'RETAIL_CSV',
    _TRANSFORM_VERSION  VARCHAR(10)     DEFAULT 'v1.0',

    CONSTRAINT PK_STG_SALES PRIMARY KEY (ORDER_ID)
);

-- ----------------------------------------------------------------------------
-- STEP 2: Populate the Staging Table from Raw Layer
-- This INSERT applies all business rules and transformations in one pass
-- ----------------------------------------------------------------------------
INSERT INTO RETAIL_DB.STAGING.STG_SALES (
    ORDER_ID,
    ORDER_DATE,
    ORDER_YEAR,
    ORDER_MONTH,
    ORDER_MONTH_NAME,
    ORDER_QUARTER,
    ORDER_DAY_OF_WEEK,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CUSTOMER_NAME_CLEAN,
    PRODUCT,
    CATEGORY,
    CATEGORY_UPPER,
    REGION,
    SALES,
    QUANTITY,
    PROFIT,
    UNIT_PRICE,
    PROFIT_RATIO,
    PROFIT_MARGIN_BAND,
    IS_PROFITABLE,
    _SOURCE_SYSTEM,
    _TRANSFORM_VERSION
)
SELECT
    -- ----------------------------------------------------------------
    -- 1. Primary Key — ensure not NULL, trim whitespace
    -- ----------------------------------------------------------------
    TRIM(UPPER(ORDER_ID))                                   AS ORDER_ID,

    -- ----------------------------------------------------------------
    -- 2. Date fields — TRY_TO_DATE safely handles malformed dates
    --    Falls back to NULL rather than throwing an error
    -- ----------------------------------------------------------------
    TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD')                   AS ORDER_DATE,
    YEAR(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))             AS ORDER_YEAR,
    MONTH(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))            AS ORDER_MONTH,
    MONTHNAME(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))        AS ORDER_MONTH_NAME,

    -- Quarter label like 'Q1-2024'
    'Q' || QUARTER(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))
        || '-' || YEAR(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))
                                                            AS ORDER_QUARTER,

    DAYNAME(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))          AS ORDER_DAY_OF_WEEK,

    -- ----------------------------------------------------------------
    -- 3. Customer fields — COALESCE handles NULLs with safe defaults
    -- ----------------------------------------------------------------
    TRIM(COALESCE(CUSTOMER_ID, 'UNKNOWN'))                  AS CUSTOMER_ID,
    COALESCE(TRIM(CUSTOMER_NAME), 'Unknown Customer')       AS CUSTOMER_NAME,

    -- Initcap converts "alice johnson" → "Alice Johnson"
    INITCAP(TRIM(COALESCE(CUSTOMER_NAME, 'Unknown Customer')))
                                                            AS CUSTOMER_NAME_CLEAN,

    -- ----------------------------------------------------------------
    -- 4. Product fields
    -- ----------------------------------------------------------------
    TRIM(COALESCE(PRODUCT,  'Unknown Product'))             AS PRODUCT,
    TRIM(COALESCE(CATEGORY, 'Uncategorized'))               AS CATEGORY,
    UPPER(TRIM(COALESCE(CATEGORY, 'UNCATEGORIZED')))        AS CATEGORY_UPPER,

    -- ----------------------------------------------------------------
    -- 5. Region — standardize to title case
    -- ----------------------------------------------------------------
    INITCAP(TRIM(COALESCE(REGION, 'Unknown')))              AS REGION,

    -- ----------------------------------------------------------------
    -- 6. Financial columns — TRY_TO_DOUBLE safely parses numeric strings
    --    COALESCE ensures NULLs become 0 instead of breaking aggregations
    -- ----------------------------------------------------------------
    COALESCE(TRY_TO_DOUBLE(SALES),    0.0)                  AS SALES,
    COALESCE(TRY_TO_NUMBER(QUANTITY), 0)                    AS QUANTITY,
    COALESCE(TRY_TO_DOUBLE(PROFIT),   0.0)                  AS PROFIT,

    -- ----------------------------------------------------------------
    -- 7. Derived columns — computed business metrics
    -- ----------------------------------------------------------------

    -- Unit Price = Sales ÷ Quantity (guard against divide-by-zero)
    CASE
        WHEN COALESCE(TRY_TO_NUMBER(QUANTITY), 0) > 0
        THEN ROUND(COALESCE(TRY_TO_DOUBLE(SALES), 0) /
                   COALESCE(TRY_TO_NUMBER(QUANTITY), 1), 2)
        ELSE 0
    END                                                     AS UNIT_PRICE,

    -- Profit Ratio = (Profit / Sales) × 100 — as a percentage
    CASE
        WHEN COALESCE(TRY_TO_DOUBLE(SALES), 0) > 0
        THEN ROUND(
            (COALESCE(TRY_TO_DOUBLE(PROFIT), 0) /
             COALESCE(TRY_TO_DOUBLE(SALES),  1)) * 100
        , 2)
        ELSE 0
    END                                                     AS PROFIT_RATIO,

    -- Profit Margin Band — business segmentation
    CASE
        WHEN COALESCE(TRY_TO_DOUBLE(SALES), 0) = 0 THEN 'No Revenue'
        WHEN (COALESCE(TRY_TO_DOUBLE(PROFIT), 0) /
              COALESCE(TRY_TO_DOUBLE(SALES), 1)) * 100 >= 30 THEN 'High'
        WHEN (COALESCE(TRY_TO_DOUBLE(PROFIT), 0) /
              COALESCE(TRY_TO_DOUBLE(SALES), 1)) * 100 >= 15 THEN 'Medium'
        ELSE 'Low'
    END                                                     AS PROFIT_MARGIN_BAND,

    -- Is Profitable flag
    CASE
        WHEN COALESCE(TRY_TO_DOUBLE(PROFIT), 0) > 0 THEN TRUE
        ELSE FALSE
    END                                                     AS IS_PROFITABLE,

    -- Audit columns
    'RETAIL_CSV'                                            AS _SOURCE_SYSTEM,
    'v1.0'                                                  AS _TRANSFORM_VERSION

FROM RETAIL_DB.RAW.RAW_SALES
WHERE
    -- Filter out records with critical missing data
    ORDER_ID IS NOT NULL
    AND TRIM(ORDER_ID) != ''
    AND TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD') IS NOT NULL;

-- ----------------------------------------------------------------------------
-- STEP 3: Post-load validation
-- ----------------------------------------------------------------------------
-- Row count check
SELECT COUNT(*) AS stg_row_count FROM RETAIL_DB.STAGING.STG_SALES;

-- Verify no NULLs in critical columns
SELECT
    SUM(CASE WHEN ORDER_ID       IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN ORDER_DATE     IS NULL THEN 1 ELSE 0 END) AS null_dates,
    SUM(CASE WHEN CUSTOMER_ID    IS NULL THEN 1 ELSE 0 END) AS null_customers,
    SUM(CASE WHEN SALES          IS NULL THEN 1 ELSE 0 END) AS null_sales,
    SUM(CASE WHEN SALES          < 0     THEN 1 ELSE 0 END) AS negative_sales
FROM RETAIL_DB.STAGING.STG_SALES;

-- Distribution check: profit margin bands
SELECT PROFIT_MARGIN_BAND, COUNT(*) AS cnt, ROUND(AVG(PROFIT_RATIO), 2) AS avg_profit_pct
FROM RETAIL_DB.STAGING.STG_SALES
GROUP BY PROFIT_MARGIN_BAND
ORDER BY avg_profit_pct DESC;

-- Preview the enriched data
SELECT
    ORDER_ID, ORDER_DATE, ORDER_QUARTER, ORDER_MONTH_NAME,
    CUSTOMER_NAME_CLEAN, PRODUCT, CATEGORY,
    SALES, QUANTITY, PROFIT,
    UNIT_PRICE, PROFIT_RATIO, PROFIT_MARGIN_BAND
FROM RETAIL_DB.STAGING.STG_SALES
LIMIT 10;
