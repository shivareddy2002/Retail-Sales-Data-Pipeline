USE DATABASE RETAIL_DATA_PLATFORM;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE DIM_CUSTOMER (
  customer_sk NUMBER AUTOINCREMENT,
  customer_id STRING,
  customer_name STRING,
  effective_from TIMESTAMP_NTZ,
  effective_to TIMESTAMP_NTZ,
  is_current BOOLEAN,
  customer_hash STRING
);

CREATE OR REPLACE TABLE DIM_PRODUCT (
  product_sk NUMBER AUTOINCREMENT,
  product_id STRING,
  product_name STRING,
  category STRING
);

CREATE OR REPLACE TABLE DIM_REGION (
  region_sk NUMBER AUTOINCREMENT,
  region_code STRING
);

CREATE OR REPLACE TABLE DIM_DATE (
  date_sk NUMBER,
  full_date DATE,
  year NUMBER, month NUMBER, day NUMBER, month_name STRING, quarter STRING
);

CREATE OR REPLACE TABLE FACT_SALES (
  sales_sk NUMBER AUTOINCREMENT,
  order_id STRING,
  date_sk NUMBER,
  customer_sk NUMBER,
  product_sk NUMBER,
  region_sk NUMBER,
  quantity NUMBER(10,0),
  sales_amount NUMBER(12,2),
  profit_amount NUMBER(12,2),
  event_ts TIMESTAMP_NTZ
)
CLUSTER BY (date_sk, region_sk);

-- SCD2 customer upsert
MERGE INTO DIM_CUSTOMER T
USING (
  SELECT DISTINCT customer_id, customer_name,
         SHA2(CONCAT_WS('|',customer_id,customer_name),256) AS customer_hash
  FROM RETAIL_DATA_PLATFORM.SILVER.SALES_CLEAN
) S
ON T.customer_id=S.customer_id AND T.is_current=TRUE
WHEN MATCHED AND T.customer_hash<>S.customer_hash THEN
  UPDATE SET is_current=FALSE, effective_to=CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (customer_id,customer_name,effective_from,effective_to,is_current,customer_hash)
  VALUES (S.customer_id,S.customer_name,CURRENT_TIMESTAMP(),'9999-12-31'::TIMESTAMP_NTZ,TRUE,S.customer_hash);

INSERT INTO DIM_PRODUCT (product_id,product_name,category)
SELECT DISTINCT product_id, product_name, category
FROM RETAIL_DATA_PLATFORM.SILVER.SALES_CLEAN s
WHERE NOT EXISTS (SELECT 1 FROM DIM_PRODUCT d WHERE d.product_id=s.product_id);

INSERT INTO DIM_REGION (region_code)
SELECT DISTINCT region FROM RETAIL_DATA_PLATFORM.SILVER.SALES_CLEAN s
WHERE NOT EXISTS (SELECT 1 FROM DIM_REGION d WHERE d.region_code=s.region);

INSERT INTO FACT_SALES (order_id,date_sk,customer_sk,product_sk,region_sk,quantity,sales_amount,profit_amount,event_ts)
SELECT s.order_id,
       TO_NUMBER(TO_CHAR(s.order_date,'YYYYMMDD')),
       c.customer_sk,p.product_sk,r.region_sk,
       s.quantity,s.sales_amount,s.profit_amount,s.event_ts
FROM RETAIL_DATA_PLATFORM.SILVER.SALES_CLEAN s
JOIN DIM_CUSTOMER c ON s.customer_id=c.customer_id AND c.is_current=TRUE
JOIN DIM_PRODUCT p ON s.product_id=p.product_id
JOIN DIM_REGION r ON s.region=r.region_code;

CREATE OR REPLACE TABLE AGG_MONTHLY_REGION_SALES AS
SELECT DATE_TRUNC('MONTH', d.full_date) AS sales_month,
       r.region_code,
       SUM(f.sales_amount) AS revenue,
       SUM(f.profit_amount) AS profit,
       SUM(f.quantity) AS units
FROM FACT_SALES f
JOIN DIM_DATE d ON f.date_sk=d.date_sk
JOIN DIM_REGION r ON f.region_sk=r.region_sk
GROUP BY 1,2;
