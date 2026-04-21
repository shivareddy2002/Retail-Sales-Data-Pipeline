-- =============================================================================
-- FILE: 08_analytical_queries/01_business_analytics.sql
-- PROJECT: Retail Sales Data Pipeline
-- DESCRIPTION: Business intelligence queries on the Star Schema
--              These power the Power BI dashboards
-- =============================================================================

USE ROLE      RETAIL_ENGINEER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE  RETAIL_DB;
USE SCHEMA    RETAIL_DB.MART;

-- ============================================================================
-- QUERY 1: Top 10 Products by Total Revenue
-- USE: Bar chart in Power BI — Products × Revenue
-- ============================================================================
SELECT
    dp.PRODUCT_NAME,
    dp.CATEGORY,
    SUM(fs.SALES_AMOUNT)                        AS TOTAL_REVENUE,
    SUM(fs.QUANTITY_SOLD)                       AS TOTAL_UNITS,
    SUM(fs.PROFIT_AMOUNT)                       AS TOTAL_PROFIT,
    ROUND(AVG(fs.PROFIT_RATIO), 2)              AS AVG_PROFIT_PCT,
    COUNT(DISTINCT fs.ORDER_ID)                 AS NUMBER_OF_ORDERS,
    -- Revenue rank within the category
    RANK() OVER (ORDER BY SUM(fs.SALES_AMOUNT) DESC) AS OVERALL_RANK
FROM RETAIL_DB.MART.FACT_SALES    fs
JOIN RETAIL_DB.MART.DIM_PRODUCT   dp ON fs.PRODUCT_SK  = dp.PRODUCT_SK
GROUP BY dp.PRODUCT_NAME, dp.CATEGORY
ORDER BY TOTAL_REVENUE DESC
LIMIT 10;

-- ============================================================================
-- QUERY 2: Region-Wise Profit & Revenue Analysis
-- USE: Map visual + bar chart in Power BI
-- ============================================================================
SELECT
    dr.REGION_NAME,
    COUNT(DISTINCT fs.ORDER_ID)                 AS TOTAL_ORDERS,
    SUM(fs.SALES_AMOUNT)                        AS TOTAL_REVENUE,
    SUM(fs.PROFIT_AMOUNT)                       AS TOTAL_PROFIT,
    ROUND(SUM(fs.SALES_AMOUNT)  /
          SUM(SUM(fs.SALES_AMOUNT)) OVER() * 100, 2)
                                                AS REVENUE_PCT_OF_TOTAL,
    ROUND(AVG(fs.PROFIT_RATIO), 2)              AS AVG_PROFIT_MARGIN_PCT,
    -- Rank regions by profit
    RANK() OVER (ORDER BY SUM(fs.PROFIT_AMOUNT) DESC)
                                                AS PROFIT_RANK
FROM RETAIL_DB.MART.FACT_SALES   fs
JOIN RETAIL_DB.MART.DIM_REGION   dr ON fs.REGION_SK = dr.REGION_SK
GROUP BY dr.REGION_NAME
ORDER BY TOTAL_PROFIT DESC;

-- ============================================================================
-- QUERY 3: Monthly Sales Trend (for Time Series / Line Chart)
-- USE: Line chart in Power BI — Month × Revenue/Profit
-- ============================================================================
SELECT
    dd.YEAR_NUMBER,
    dd.MONTH_NUMBER,
    dd.MONTH_NAME,
    dd.YEAR_MONTH,                              -- e.g. '2024-01' for sorting
    COUNT(DISTINCT fs.ORDER_ID)                 AS ORDERS_COUNT,
    SUM(fs.SALES_AMOUNT)                        AS MONTHLY_REVENUE,
    SUM(fs.PROFIT_AMOUNT)                       AS MONTHLY_PROFIT,
    ROUND(AVG(fs.PROFIT_RATIO), 2)              AS AVG_PROFIT_MARGIN,
    -- Month-over-month revenue growth
    LAG(SUM(fs.SALES_AMOUNT)) OVER
        (ORDER BY dd.YEAR_NUMBER, dd.MONTH_NUMBER)
                                                AS PREV_MONTH_REVENUE,
    ROUND(
        (SUM(fs.SALES_AMOUNT) -
         LAG(SUM(fs.SALES_AMOUNT)) OVER (ORDER BY dd.YEAR_NUMBER, dd.MONTH_NUMBER))
        / NULLIF(LAG(SUM(fs.SALES_AMOUNT)) OVER
            (ORDER BY dd.YEAR_NUMBER, dd.MONTH_NUMBER), 0) * 100
    , 2)                                        AS MOM_GROWTH_PCT
FROM RETAIL_DB.MART.FACT_SALES  fs
JOIN RETAIL_DB.MART.DIM_DATE    dd ON fs.DATE_SK = dd.DATE_SK
GROUP BY dd.YEAR_NUMBER, dd.MONTH_NUMBER, dd.MONTH_NAME, dd.YEAR_MONTH
ORDER BY dd.YEAR_NUMBER, dd.MONTH_NUMBER;

-- ============================================================================
-- QUERY 4: Customer Segmentation & RFM Analysis
-- USE: Pie chart or table in Power BI — Customer value tiers
-- ============================================================================
WITH CUSTOMER_METRICS AS (
    SELECT
        dc.CUSTOMER_ID,
        dc.CUSTOMER_NAME,
        dc.CUSTOMER_SEGMENT,
        COUNT(DISTINCT fs.ORDER_ID)             AS FREQUENCY,        -- F
        SUM(fs.SALES_AMOUNT)                    AS MONETARY,         -- M
        MAX(dd.FULL_DATE)                       AS LAST_ORDER_DATE,  -- used for Recency
        DATEDIFF('day', MAX(dd.FULL_DATE), CURRENT_DATE()) AS RECENCY_DAYS  -- R
    FROM RETAIL_DB.MART.FACT_SALES   fs
    JOIN RETAIL_DB.MART.DIM_CUSTOMER dc ON fs.CUSTOMER_SK = dc.CUSTOMER_SK
    JOIN RETAIL_DB.MART.DIM_DATE     dd ON fs.DATE_SK     = dd.DATE_SK
    GROUP BY dc.CUSTOMER_ID, dc.CUSTOMER_NAME, dc.CUSTOMER_SEGMENT
)
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    FREQUENCY,
    MONETARY,
    RECENCY_DAYS,
    CUSTOMER_SEGMENT,
    -- Assign RFM tiers
    CASE
        WHEN MONETARY    >= 400 AND FREQUENCY >= 2 THEN 'Champions'
        WHEN MONETARY    >= 200 AND RECENCY_DAYS <= 60 THEN 'Loyal Customers'
        WHEN RECENCY_DAYS <= 30 AND FREQUENCY = 1 THEN 'New Customers'
        WHEN RECENCY_DAYS >= 90 THEN 'At Risk'
        ELSE 'Potential Loyalist'
    END                                         AS RFM_SEGMENT,
    RANK() OVER (ORDER BY MONETARY DESC)        AS REVENUE_RANK
FROM CUSTOMER_METRICS
ORDER BY MONETARY DESC;

-- ============================================================================
-- QUERY 5: Category Performance Breakdown
-- USE: Stacked bar chart — Category × Revenue by Quarter
-- ============================================================================
SELECT
    dd.YEAR_QUARTER,
    dp.CATEGORY,
    SUM(fs.SALES_AMOUNT)                        AS REVENUE,
    SUM(fs.PROFIT_AMOUNT)                       AS PROFIT,
    COUNT(DISTINCT fs.ORDER_ID)                 AS ORDERS,
    ROUND(SUM(fs.SALES_AMOUNT) /
          SUM(SUM(fs.SALES_AMOUNT)) OVER (PARTITION BY dd.YEAR_QUARTER) * 100
    , 2)                                        AS CATEGORY_SHARE_PCT
FROM RETAIL_DB.MART.FACT_SALES  fs
JOIN RETAIL_DB.MART.DIM_DATE    dd ON fs.DATE_SK    = dd.DATE_SK
JOIN RETAIL_DB.MART.DIM_PRODUCT dp ON fs.PRODUCT_SK = dp.PRODUCT_SK
GROUP BY dd.YEAR_QUARTER, dp.CATEGORY
ORDER BY dd.YEAR_QUARTER, REVENUE DESC;

-- ============================================================================
-- QUERY 6: KPI Summary — Single Row Dashboard Numbers
-- USE: KPI cards in Power BI
-- ============================================================================
SELECT
    COUNT(DISTINCT ORDER_ID)                    AS TOTAL_ORDERS,
    COUNT(DISTINCT CUSTOMER_SK)                 AS UNIQUE_CUSTOMERS,
    ROUND(SUM(SALES_AMOUNT), 2)                 AS TOTAL_REVENUE,
    ROUND(SUM(PROFIT_AMOUNT), 2)                AS TOTAL_PROFIT,
    ROUND(AVG(PROFIT_RATIO), 2)                 AS AVG_PROFIT_MARGIN_PCT,
    ROUND(SUM(SALES_AMOUNT) /
          COUNT(DISTINCT ORDER_ID), 2)          AS AVG_ORDER_VALUE,
    SUM(QUANTITY_SOLD)                          AS TOTAL_UNITS_SOLD
FROM RETAIL_DB.MART.FACT_SALES;

-- ============================================================================
-- QUERY 7: Sales vs Profit Scatter Data (per order)
-- USE: Scatter chart in Power BI
-- ============================================================================
SELECT
    fs.ORDER_ID,
    dp.PRODUCT_NAME,
    dp.CATEGORY,
    dr.REGION_NAME,
    dd.YEAR_QUARTER,
    fs.SALES_AMOUNT,
    fs.PROFIT_AMOUNT,
    fs.PROFIT_RATIO,
    fs.QUANTITY_SOLD
FROM RETAIL_DB.MART.FACT_SALES  fs
JOIN RETAIL_DB.MART.DIM_PRODUCT dp ON fs.PRODUCT_SK = dp.PRODUCT_SK
JOIN RETAIL_DB.MART.DIM_REGION  dr ON fs.REGION_SK  = dr.REGION_SK
JOIN RETAIL_DB.MART.DIM_DATE    dd ON fs.DATE_SK     = dd.DATE_SK
ORDER BY fs.SALES_AMOUNT DESC;
