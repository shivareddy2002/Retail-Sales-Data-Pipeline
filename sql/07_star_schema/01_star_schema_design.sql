-- =============================================================================
-- FILE: 07_star_schema/01_star_schema_design.sql
-- PROJECT: Retail Sales Data Pipeline
-- DESCRIPTION: Star Schema dimensional model for BI and analytics
--
-- STAR SCHEMA EXPLAINED:
-- ─────────────────────────────────────────────────────────────────────────────
-- A Star Schema organizes data into:
--   FACT TABLE   → Contains measurable business events (sales transactions)
--                  with foreign keys to dimension tables
--   DIMENSION    → Contains descriptive attributes (who, what, where, when)
--
-- WHY STAR SCHEMA?
--   ✓ Optimized for analytical queries (aggregations, GROUP BY)
--   ✓ Simple joins — BI tools like Power BI handle it natively
--   ✓ Easy for business users to understand
--   ✓ Better query performance than normalized (3NF) models
--   ✓ Industry standard for data warehouses (Kimball methodology)
--
-- Our Star Schema:
--
--         DIM_DATE ──────┐
--         DIM_CUSTOMER ──┤
--                        ├──→ FACT_SALES
--         DIM_PRODUCT  ──┤
--         DIM_REGION   ──┘
-- ─────────────────────────────────────────────────────────────────────────────

USE ROLE      RETAIL_ENGINEER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE  RETAIL_DB;
USE SCHEMA    RETAIL_DB.MART;

-- ============================================================================
-- DIMENSION 1: DIM_DATE
-- The Date dimension is pre-populated for a date range (calendar table)
-- Every possible date gets a surrogate key and rich calendar attributes
-- ============================================================================
CREATE OR REPLACE TABLE RETAIL_DB.MART.DIM_DATE (
    DATE_SK         NUMBER          NOT NULL    COMMENT 'Surrogate key (integer: YYYYMMDD)',
    FULL_DATE       DATE            NOT NULL    COMMENT 'The actual date value',
    DAY_OF_MONTH    NUMBER(2)                   COMMENT '1–31',
    DAY_NAME        VARCHAR(15)                 COMMENT 'Monday, Tuesday, ...',
    DAY_OF_WEEK     NUMBER(1)                   COMMENT '1=Sunday ... 7=Saturday',
    IS_WEEKEND      BOOLEAN                     COMMENT 'TRUE if Saturday or Sunday',
    WEEK_OF_YEAR    NUMBER(2)                   COMMENT '1–53',
    MONTH_NUMBER    NUMBER(2)                   COMMENT '1–12',
    MONTH_NAME      VARCHAR(15)                 COMMENT 'January, February, ...',
    MONTH_SHORT     VARCHAR(3)                  COMMENT 'Jan, Feb, ...',
    QUARTER         NUMBER(1)                   COMMENT '1–4',
    QUARTER_LABEL   VARCHAR(6)                  COMMENT 'Q1, Q2, Q3, Q4',
    YEAR_NUMBER     NUMBER(4)                   COMMENT '2023, 2024, ...',
    YEAR_QUARTER    VARCHAR(10)                 COMMENT 'Q1-2024',
    YEAR_MONTH      VARCHAR(10)                 COMMENT '2024-01',
    IS_HOLIDAY      BOOLEAN         DEFAULT FALSE COMMENT 'Manually populated for holidays',

    CONSTRAINT PK_DIM_DATE PRIMARY KEY (DATE_SK)
);

-- Populate DIM_DATE for 2023-01-01 to 2025-12-31
INSERT INTO RETAIL_DB.MART.DIM_DATE
WITH DATE_SPINE AS (
    -- Generate a row for every date in the range using a recursive CTE
    SELECT DATEADD('day', SEQ4(), '2023-01-01'::DATE) AS FULL_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 1095))  -- ~3 years of days
    WHERE FULL_DATE <= '2025-12-31'
)
SELECT
    YEAR(FULL_DATE) * 10000
        + MONTH(FULL_DATE) * 100
        + DAY(FULL_DATE)                    AS DATE_SK,          -- e.g. 20240115
    FULL_DATE,
    DAY(FULL_DATE)                          AS DAY_OF_MONTH,
    DAYNAME(FULL_DATE)                      AS DAY_NAME,
    DAYOFWEEK(FULL_DATE)                    AS DAY_OF_WEEK,
    CASE WHEN DAYOFWEEK(FULL_DATE) IN (0,6) THEN TRUE
         ELSE FALSE END                     AS IS_WEEKEND,
    WEEKOFYEAR(FULL_DATE)                   AS WEEK_OF_YEAR,
    MONTH(FULL_DATE)                        AS MONTH_NUMBER,
    MONTHNAME(FULL_DATE)                    AS MONTH_NAME,
    LEFT(MONTHNAME(FULL_DATE), 3)           AS MONTH_SHORT,
    QUARTER(FULL_DATE)                      AS QUARTER,
    'Q' || QUARTER(FULL_DATE)               AS QUARTER_LABEL,
    YEAR(FULL_DATE)                         AS YEAR_NUMBER,
    'Q' || QUARTER(FULL_DATE) || '-' || YEAR(FULL_DATE)
                                            AS YEAR_QUARTER,
    TO_CHAR(FULL_DATE, 'YYYY-MM')           AS YEAR_MONTH,
    FALSE                                   AS IS_HOLIDAY
FROM DATE_SPINE;

-- ============================================================================
-- DIMENSION 2: DIM_CUSTOMER
-- One row per unique customer with latest attributes (SCD Type 1)
-- ============================================================================
CREATE OR REPLACE TABLE RETAIL_DB.MART.DIM_CUSTOMER (
    CUSTOMER_SK         NUMBER AUTOINCREMENT COMMENT 'Surrogate key (system generated)',
    CUSTOMER_ID         VARCHAR(20)  NOT NULL COMMENT 'Business key from source system',
    CUSTOMER_NAME       VARCHAR(100)          COMMENT 'Full name (title-cased)',
    FIRST_PURCHASE_DATE DATE                  COMMENT 'Date of customer first order',
    LAST_PURCHASE_DATE  DATE                  COMMENT 'Date of most recent order',
    TOTAL_ORDERS        NUMBER        DEFAULT 0,
    CUSTOMER_SEGMENT    VARCHAR(20)           COMMENT 'High Value / Mid Tier / Regular',
    _CREATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _UPDATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_CUSTOMER PRIMARY KEY (CUSTOMER_SK),
    CONSTRAINT UQ_DIM_CUSTOMER UNIQUE (CUSTOMER_ID)
);

-- Populate DIM_CUSTOMER from staging
INSERT INTO RETAIL_DB.MART.DIM_CUSTOMER (
    CUSTOMER_ID, CUSTOMER_NAME, FIRST_PURCHASE_DATE,
    LAST_PURCHASE_DATE, TOTAL_ORDERS, CUSTOMER_SEGMENT
)
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME_CLEAN,
    MIN(ORDER_DATE)     AS FIRST_PURCHASE_DATE,
    MAX(ORDER_DATE)     AS LAST_PURCHASE_DATE,
    COUNT(*)            AS TOTAL_ORDERS,
    -- RFM-style customer segmentation based on order count
    CASE
        WHEN COUNT(*) >= 3 THEN 'High Value'
        WHEN COUNT(*) >= 2 THEN 'Mid Tier'
        ELSE 'Regular'
    END                 AS CUSTOMER_SEGMENT
FROM RETAIL_DB.STAGING.STG_SALES
GROUP BY CUSTOMER_ID, CUSTOMER_NAME_CLEAN;

-- ============================================================================
-- DIMENSION 3: DIM_PRODUCT
-- One row per unique product with category info
-- ============================================================================
CREATE OR REPLACE TABLE RETAIL_DB.MART.DIM_PRODUCT (
    PRODUCT_SK      NUMBER AUTOINCREMENT COMMENT 'Surrogate key',
    PRODUCT_NAME    VARCHAR(200) NOT NULL COMMENT 'Product name from source',
    CATEGORY        VARCHAR(100)          COMMENT 'Product category',
    CATEGORY_UPPER  VARCHAR(100)          COMMENT 'Standardized uppercase category',
    AVG_UNIT_PRICE  FLOAT                 COMMENT 'Average selling price',
    _CREATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_PRODUCT PRIMARY KEY (PRODUCT_SK),
    CONSTRAINT UQ_DIM_PRODUCT UNIQUE (PRODUCT_NAME)
);

INSERT INTO RETAIL_DB.MART.DIM_PRODUCT (PRODUCT_NAME, CATEGORY, CATEGORY_UPPER, AVG_UNIT_PRICE)
SELECT DISTINCT
    PRODUCT,
    CATEGORY,
    CATEGORY_UPPER,
    AVG(UNIT_PRICE) OVER (PARTITION BY PRODUCT) AS AVG_UNIT_PRICE
FROM RETAIL_DB.STAGING.STG_SALES
QUALIFY ROW_NUMBER() OVER (PARTITION BY PRODUCT ORDER BY ORDER_DATE DESC) = 1;

-- ============================================================================
-- DIMENSION 4: DIM_REGION
-- Geographical dimension — can be enriched with lat/long in production
-- ============================================================================
CREATE OR REPLACE TABLE RETAIL_DB.MART.DIM_REGION (
    REGION_SK       NUMBER AUTOINCREMENT COMMENT 'Surrogate key',
    REGION_NAME     VARCHAR(50)  NOT NULL COMMENT 'Region name (North/South/East/West)',
    REGION_GROUP    VARCHAR(50)           COMMENT 'Macro region grouping',
    COUNTRY         VARCHAR(50)  DEFAULT 'United States',
    _CREATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_REGION  PRIMARY KEY (REGION_SK),
    CONSTRAINT UQ_DIM_REGION  UNIQUE (REGION_NAME)
);

INSERT INTO RETAIL_DB.MART.DIM_REGION (REGION_NAME, REGION_GROUP, COUNTRY)
VALUES
    ('North', 'Northern Territory', 'United States'),
    ('South', 'Southern Territory', 'United States'),
    ('East',  'Eastern Territory',  'United States'),
    ('West',  'Western Territory',  'United States');

-- ============================================================================
-- FACT TABLE: FACT_SALES
-- Central table containing one row per order transaction
-- References all four dimension tables via surrogate keys
-- ============================================================================
CREATE OR REPLACE TABLE RETAIL_DB.MART.FACT_SALES (
    -- Surrogate + Natural key
    FACT_SK         NUMBER AUTOINCREMENT  COMMENT 'Surrogate key for the fact row',
    ORDER_ID        VARCHAR(20)  NOT NULL COMMENT 'Natural business key',

    -- Foreign Keys to Dimensions
    CUSTOMER_SK     NUMBER                COMMENT 'FK → DIM_CUSTOMER',
    PRODUCT_SK      NUMBER                COMMENT 'FK → DIM_PRODUCT',
    REGION_SK       NUMBER                COMMENT 'FK → DIM_REGION',
    DATE_SK         NUMBER                COMMENT 'FK → DIM_DATE (YYYYMMDD integer)',

    -- Measures (additive facts)
    SALES_AMOUNT    FLOAT                 COMMENT 'Total order sales amount',
    QUANTITY_SOLD   INT                   COMMENT 'Number of units sold',
    PROFIT_AMOUNT   FLOAT                 COMMENT 'Profit for this order',
    UNIT_PRICE      FLOAT                 COMMENT 'Per-unit selling price',
    PROFIT_RATIO    FLOAT                 COMMENT 'Profit as % of sales',

    -- Audit
    _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_FACT_SALES  PRIMARY KEY (FACT_SK),
    CONSTRAINT UQ_FACT_ORDER  UNIQUE (ORDER_ID),
    CONSTRAINT FK_CUSTOMER    FOREIGN KEY (CUSTOMER_SK) REFERENCES RETAIL_DB.MART.DIM_CUSTOMER(CUSTOMER_SK),
    CONSTRAINT FK_PRODUCT     FOREIGN KEY (PRODUCT_SK)  REFERENCES RETAIL_DB.MART.DIM_PRODUCT(PRODUCT_SK),
    CONSTRAINT FK_REGION      FOREIGN KEY (REGION_SK)   REFERENCES RETAIL_DB.MART.DIM_REGION(REGION_SK),
    CONSTRAINT FK_DATE        FOREIGN KEY (DATE_SK)     REFERENCES RETAIL_DB.MART.DIM_DATE(DATE_SK)
);

-- Populate FACT_SALES by joining staging to dimension surrogate keys
INSERT INTO RETAIL_DB.MART.FACT_SALES (
    ORDER_ID,
    CUSTOMER_SK,
    PRODUCT_SK,
    REGION_SK,
    DATE_SK,
    SALES_AMOUNT,
    QUANTITY_SOLD,
    PROFIT_AMOUNT,
    UNIT_PRICE,
    PROFIT_RATIO
)
SELECT
    s.ORDER_ID,
    dc.CUSTOMER_SK,
    dp.PRODUCT_SK,
    dr.REGION_SK,
    dd.DATE_SK,
    s.SALES,
    s.QUANTITY,
    s.PROFIT,
    s.UNIT_PRICE,
    s.PROFIT_RATIO
FROM RETAIL_DB.STAGING.STG_SALES s
LEFT JOIN RETAIL_DB.MART.DIM_CUSTOMER dc ON s.CUSTOMER_ID = dc.CUSTOMER_ID
LEFT JOIN RETAIL_DB.MART.DIM_PRODUCT  dp ON s.PRODUCT     = dp.PRODUCT_NAME
LEFT JOIN RETAIL_DB.MART.DIM_REGION   dr ON s.REGION      = dr.REGION_NAME
LEFT JOIN RETAIL_DB.MART.DIM_DATE     dd ON s.ORDER_DATE  = dd.FULL_DATE;

-- ============================================================================
-- Validate Star Schema
-- ============================================================================
SELECT 'DIM_DATE'     AS TABLE_NAME, COUNT(*) AS ROWS FROM RETAIL_DB.MART.DIM_DATE
UNION ALL
SELECT 'DIM_CUSTOMER',  COUNT(*) FROM RETAIL_DB.MART.DIM_CUSTOMER
UNION ALL
SELECT 'DIM_PRODUCT',   COUNT(*) FROM RETAIL_DB.MART.DIM_PRODUCT
UNION ALL
SELECT 'DIM_REGION',    COUNT(*) FROM RETAIL_DB.MART.DIM_REGION
UNION ALL
SELECT 'FACT_SALES',    COUNT(*) FROM RETAIL_DB.MART.FACT_SALES;

-- Check for orphaned fact rows (FK violations)
SELECT COUNT(*) AS orphaned_customers
FROM RETAIL_DB.MART.FACT_SALES WHERE CUSTOMER_SK IS NULL;
