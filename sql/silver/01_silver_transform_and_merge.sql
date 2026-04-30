USE DATABASE RETAIL_DATA_PLATFORM;
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE SALES_CLEAN (
  order_id STRING,
  order_date DATE,
  customer_id STRING,
  customer_name STRING,
  product_id STRING,
  product_name STRING,
  category STRING,
  region STRING,
  quantity NUMBER(10,0),
  unit_price NUMBER(12,2),
  sales_amount NUMBER(12,2),
  profit_amount NUMBER(12,2),
  event_ts TIMESTAMP_NTZ,
  record_source STRING,
  ingested_at TIMESTAMP_NTZ,
  row_hash STRING,
  CONSTRAINT PK_SALES_CLEAN UNIQUE(order_id)
);

-- Stage table for dedup + late arriving handling
CREATE OR REPLACE TEMP TABLE TMP_SALES_DELTA AS
SELECT
  payload:order_id::STRING AS order_id,
  TRY_TO_DATE(payload:order_date::STRING) AS order_date,
  payload:customer_id::STRING AS customer_id,
  INITCAP(payload:customer_name::STRING) AS customer_name,
  payload:product_id::STRING AS product_id,
  payload:product_name::STRING AS product_name,
  payload:category::STRING AS category,
  UPPER(payload:region::STRING) AS region,
  COALESCE(TRY_TO_NUMBER(payload:quantity::STRING),0) AS quantity,
  COALESCE(TRY_TO_NUMBER(payload:unit_price::STRING),0) AS unit_price,
  COALESCE(TRY_TO_NUMBER(payload:sales_amount::STRING),0) AS sales_amount,
  COALESCE(TRY_TO_NUMBER(payload:profit_amount::STRING),0) AS profit_amount,
  COALESCE(TRY_TO_TIMESTAMP_NTZ(payload:event_ts::STRING), src_loaded_at) AS event_ts,
  'CSV' AS record_source,
  CURRENT_TIMESTAMP() AS ingested_at,
  SHA2(CONCAT_WS('|',payload:order_id::STRING,payload:event_ts::STRING,payload:sales_amount::STRING),256) AS row_hash
FROM STRM_BRONZE_CSV_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY payload:order_id::STRING ORDER BY COALESCE(TRY_TO_TIMESTAMP_NTZ(payload:event_ts::STRING), src_loaded_at) DESC)=1;

MERGE INTO SALES_CLEAN T
USING TMP_SALES_DELTA S
ON T.order_id = S.order_id
WHEN MATCHED AND S.event_ts >= T.event_ts AND S.row_hash <> T.row_hash THEN
  UPDATE SET
    order_date=S.order_date,customer_id=S.customer_id,customer_name=S.customer_name,
    product_id=S.product_id,product_name=S.product_name,category=S.category,region=S.region,
    quantity=S.quantity,unit_price=S.unit_price,sales_amount=S.sales_amount,profit_amount=S.profit_amount,
    event_ts=S.event_ts,record_source=S.record_source,ingested_at=S.ingested_at,row_hash=S.row_hash
WHEN NOT MATCHED THEN
  INSERT VALUES (S.order_id,S.order_date,S.customer_id,S.customer_name,S.product_id,S.product_name,S.category,S.region,S.quantity,S.unit_price,S.sales_amount,S.profit_amount,S.event_ts,S.record_source,S.ingested_at,S.row_hash);
