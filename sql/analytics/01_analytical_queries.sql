-- Top products by revenue
SELECT p.product_name, SUM(f.sales_amount) AS revenue
FROM GOLD.FACT_SALES f JOIN GOLD.DIM_PRODUCT p ON f.product_sk=p.product_sk
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- Region-wise performance
SELECT r.region_code, SUM(f.sales_amount) revenue, SUM(f.profit_amount) profit
FROM GOLD.FACT_SALES f JOIN GOLD.DIM_REGION r ON f.region_sk=r.region_sk
GROUP BY 1 ORDER BY 2 DESC;

-- Monthly trends
SELECT d.year, d.month, SUM(f.sales_amount) monthly_revenue
FROM GOLD.FACT_SALES f JOIN GOLD.DIM_DATE d ON f.date_sk=d.date_sk
GROUP BY 1,2 ORDER BY 1,2;

-- Customer segmentation (RFM-lite)
SELECT c.customer_name,
       COUNT(DISTINCT f.order_id) orders,
       SUM(f.sales_amount) lifetime_value,
       DATEDIFF('day', MAX(d.full_date), CURRENT_DATE()) recency_days
FROM GOLD.FACT_SALES f
JOIN GOLD.DIM_CUSTOMER c ON f.customer_sk=c.customer_sk
JOIN GOLD.DIM_DATE d ON f.date_sk=d.date_sk
GROUP BY 1
ORDER BY lifetime_value DESC;
