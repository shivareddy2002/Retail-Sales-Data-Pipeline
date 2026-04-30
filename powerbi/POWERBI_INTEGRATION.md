# Power BI Integration Guide

1. Open Power BI Desktop → **Get Data** → **Snowflake**.
2. Enter account URL (`<org>-<acct>.snowflakecomputing.com`), warehouse (`RETAIL_WH_XS`), role (`RETAIL_BI_ROLE`), database (`RETAIL_DATA_PLATFORM`).
3. Import these Gold objects:
   - `GOLD.FACT_SALES`
   - `GOLD.DIM_CUSTOMER`
   - `GOLD.DIM_PRODUCT`
   - `GOLD.DIM_REGION`
   - `GOLD.DIM_DATE`
   - Optional: `GOLD.AGG_MONTHLY_REGION_SALES`
4. Build model relationships:
   - FACT_SALES (many) to each DIM table (one) on surrogate keys.
5. Dashboard layout:
   - KPI cards: Revenue, Profit, Orders, Avg Order Value.
   - Bar chart: Top 10 products by revenue.
   - Line chart: Monthly revenue trend.
   - Filled map: Region-wise sales.
6. Publish to Power BI Service; configure scheduled refresh (every 30 minutes or DirectQuery).
