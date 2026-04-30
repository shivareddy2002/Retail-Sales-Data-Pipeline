-- Performance standards
-- 1) Warehouse sizing strategy:
--    * RETAIL_WH_XS for BI/low concurrency
--    * RETAIL_WH_ETL SMALL for transformations
--    * Scale to MEDIUM during backfill windows

-- 2) Cluster large fact table by date and region (already defined in DDL)
ALTER TABLE RETAIL_DATA_PLATFORM.GOLD.FACT_SALES CLUSTER BY (date_sk, region_sk);

-- 3) Search optimization for selective predicates
ALTER TABLE RETAIL_DATA_PLATFORM.GOLD.FACT_SALES
  ADD SEARCH OPTIMIZATION ON EQUALITY(order_id, customer_sk, product_sk);

-- 4) Materialized view for hot aggregation
CREATE OR REPLACE MATERIALIZED VIEW RETAIL_DATA_PLATFORM.GOLD.MV_DAILY_REVENUE AS
SELECT date_sk, SUM(sales_amount) AS revenue, SUM(profit_amount) AS profit
FROM RETAIL_DATA_PLATFORM.GOLD.FACT_SALES
GROUP BY date_sk;
