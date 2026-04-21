-- =============================================================================
-- FILE: 02_raw_layer/01_raw_table_and_stage.sql
-- PROJECT: Retail Sales Data Pipeline
-- DESCRIPTION: Create raw landing table, internal stage, and file format
--              then load CSV data using COPY INTO
-- =============================================================================

USE ROLE      RETAIL_ENGINEER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE  RETAIL_DB;
USE SCHEMA    RETAIL_DB.RAW;

-- ----------------------------------------------------------------------------
-- STEP 1: Create the Raw Sales Table
-- PURPOSE: Mirror the source CSV exactly — no business logic here
-- NOTE: All columns are VARCHAR initially to avoid load failures
--       Type casting happens in the transformation layer
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RETAIL_DB.RAW.RAW_SALES (
    ORDER_ID        VARCHAR(20)     NOT NULL COMMENT 'Unique order identifier from source system',
    ORDER_DATE      VARCHAR(20)              COMMENT 'Order date as string (YYYY-MM-DD) — cast later',
    CUSTOMER_ID     VARCHAR(20)              COMMENT 'Customer identifier',
    CUSTOMER_NAME   VARCHAR(100)             COMMENT 'Full name of the customer',
    PRODUCT         VARCHAR(200)             COMMENT 'Product name',
    CATEGORY        VARCHAR(100)             COMMENT 'Product category',
    REGION          VARCHAR(50)              COMMENT 'Sales region (North/South/East/West)',
    SALES           VARCHAR(20)              COMMENT 'Total sales amount — cast to FLOAT later',
    QUANTITY        VARCHAR(10)              COMMENT 'Number of units sold — cast to INT later',
    PROFIT          VARCHAR(20)              COMMENT 'Profit amount — cast to FLOAT later',
    -- Metadata columns added at load time
    _LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'When this record was loaded into Snowflake',
    _FILE_NAME      VARCHAR(500)             COMMENT 'Source file name for audit trail',
    _ROW_NUMBER     NUMBER                   COMMENT 'Row number within the source file'
);

-- ----------------------------------------------------------------------------
-- STEP 2: Create an Internal Named Stage
-- PURPOSE: Acts as a Snowflake-managed S3 bucket where we upload CSV files
--          before loading them into tables. Avoids needing external cloud storage.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE STAGE RETAIL_DB.RAW.RETAIL_STAGE
    COMMENT = 'Internal stage for retail CSV file uploads'
    DIRECTORY = ( ENABLE = TRUE );

-- ----------------------------------------------------------------------------
-- STEP 3: Define a File Format for CSV parsing
-- PURPOSE: Tells Snowflake how to interpret the CSV file
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT RETAIL_DB.RAW.CSV_FORMAT
    TYPE              = 'CSV'
    FIELD_DELIMITER   = ','
    RECORD_DELIMITER  = '\n'
    SKIP_HEADER       = 1           -- Skip the header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'  -- Handle quoted fields
    NULL_IF           = ('NULL', 'null', 'N/A', '', 'NA')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE        = TRUE        -- Strip leading/trailing whitespace
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Standard CSV format for retail data files';

-- ----------------------------------------------------------------------------
-- STEP 4: Upload file to stage (run this in Snowflake UI or SnowSQL CLI)
-- COMMAND: PUT file:///<local_path>/retail_sales_raw.csv @RETAIL_STAGE;
-- Example:
--   PUT file:///Users/yourname/retail-data-pipeline/data/retail_sales_raw.csv
--       @RETAIL_DB.RAW.RETAIL_STAGE AUTO_COMPRESS=TRUE;
-- ----------------------------------------------------------------------------

-- Verify file was uploaded successfully
-- LIST @RETAIL_DB.RAW.RETAIL_STAGE;

-- ----------------------------------------------------------------------------
-- STEP 5: COPY INTO — Load CSV from Stage into Raw Table
-- PURPOSE: Bulk loads data with full metadata and error handling
-- ON_ERROR: CONTINUE means bad rows are skipped and logged (not fail entire load)
-- ----------------------------------------------------------------------------
COPY INTO RETAIL_DB.RAW.RAW_SALES (
    ORDER_ID,
    ORDER_DATE,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    PRODUCT,
    CATEGORY,
    REGION,
    SALES,
    QUANTITY,
    PROFIT,
    _FILE_NAME,
    _ROW_NUMBER
)
FROM (
    SELECT
        $1,                           -- ORDER_ID
        $2,                           -- ORDER_DATE
        $3,                           -- CUSTOMER_ID
        $4,                           -- CUSTOMER_NAME
        $5,                           -- PRODUCT
        $6,                           -- CATEGORY
        $7,                           -- REGION
        $8,                           -- SALES
        $9,                           -- QUANTITY
        $10,                          -- PROFIT
        METADATA$FILENAME,            -- Source file name
        METADATA$FILE_ROW_NUMBER      -- Row number in file
    FROM @RETAIL_DB.RAW.RETAIL_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'RETAIL_DB.RAW.CSV_FORMAT')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;   -- Keep files in stage for reprocessing if needed

-- ----------------------------------------------------------------------------
-- STEP 6: Validate the load
-- ----------------------------------------------------------------------------
-- Check row count
SELECT COUNT(*) AS total_rows_loaded FROM RETAIL_DB.RAW.RAW_SALES;

-- Preview first 10 rows
SELECT * FROM RETAIL_DB.RAW.RAW_SALES LIMIT 10;

-- Check for any NULL order IDs (critical field)
SELECT COUNT(*) AS null_order_ids
FROM RETAIL_DB.RAW.RAW_SALES
WHERE ORDER_ID IS NULL;

-- View load history (last 14 days)
SELECT
    FILE_NAME,
    STATUS,
    ROWS_PARSED,
    ROWS_LOADED,
    ERRORS_SEEN,
    FIRST_ERROR
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME    => 'RAW_SALES',
        START_TIME    => DATEADD('hour', -1, CURRENT_TIMESTAMP())
    )
);
