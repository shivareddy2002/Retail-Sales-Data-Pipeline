-- Bronze = raw, append-first, minimal transformation
USE DATABASE RETAIL_DATA_PLATFORM;
USE SCHEMA BRONZE;

CREATE OR REPLACE FILE FORMAT FF_CSV_RETAIL
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', '');

CREATE OR REPLACE STAGE STG_RETAIL_CSV
  FILE_FORMAT = FF_CSV_RETAIL;

-- Simulated sources: csv/api/stream
CREATE OR REPLACE TABLE BRONZE_SALES_CSV_RAW (
  src_filename STRING,
  src_loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  payload VARIANT
);

CREATE OR REPLACE TABLE BRONZE_SALES_API_RAW (
  src_loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  payload VARIANT
);

CREATE OR REPLACE TABLE BRONZE_SALES_STREAM_RAW (
  src_loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  payload VARIANT
);

-- Load CSV rows into semi-structured payload for schema drift tolerance
COPY INTO BRONZE_SALES_CSV_RAW (src_filename, payload)
FROM (
  SELECT METADATA$FILENAME,
         OBJECT_CONSTRUCT(
           'order_id',$1,'order_date',$2,'customer_id',$3,'customer_name',$4,
           'product_id',$5,'product_name',$6,'category',$7,'region',$8,
           'quantity',$9,'unit_price',$10,'sales_amount',$11,'profit_amount',$12,
           'event_ts',$13
         )
  FROM @STG_RETAIL_CSV
)
ON_ERROR='CONTINUE';
