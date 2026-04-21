-- =============================================================================
-- FILE: 04_cdc_streams/01_cdc_stream_setup.sql
-- PROJECT: Retail Sales Data Pipeline
-- DESCRIPTION: Change Data Capture (CDC) using Snowflake Streams
--
-- HOW SNOWFLAKE STREAMS WORK:
-- ─────────────────────────────────────────────────────────────────────────────
-- A Stream is a Snowflake object that records DML changes (INSERT, UPDATE,
-- DELETE) made to a source table SINCE the last time the stream was consumed.
--
-- Think of it like a transaction log / change feed.
--
-- Key columns added by the stream:
--   METADATA$ACTION      → 'INSERT' or 'DELETE' (UPDATE = DELETE + INSERT pair)
--   METADATA$ISUPDATE    → TRUE if this row is part of an UPDATE operation
--   METADATA$ROW_ID      → Unique internal row identifier for the source row
--
-- Stream consumption:
--   Reading a stream in a DML statement (INSERT / MERGE / UPDATE) advances the
--   stream offset. Uncommitted reads do NOT advance the offset.
-- ─────────────────────────────────────────────────────────────────────────────

USE ROLE      RETAIL_ENGINEER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE  RETAIL_DB;

-- ----------------------------------------------------------------------------
-- STEP 1: Create a Stream on the RAW_SALES table
-- APPEND_ONLY = FALSE → Captures INSERTs, UPDATEs, and DELETEs
-- APPEND_ONLY = TRUE  → Captures INSERTs only (more efficient for pure ingestion)
-- We use APPEND_ONLY = FALSE for full CDC capability
-- ----------------------------------------------------------------------------
CREATE OR REPLACE STREAM RETAIL_DB.RAW.SALES_CDC_STREAM
    ON TABLE RETAIL_DB.RAW.RAW_SALES
    APPEND_ONLY = FALSE   -- Full CDC: captures inserts, updates, deletes
    COMMENT = 'CDC stream tracking all changes on RAW_SALES table';

-- ----------------------------------------------------------------------------
-- STEP 2: Verify the stream was created
-- ----------------------------------------------------------------------------
SHOW STREAMS IN SCHEMA RETAIL_DB.RAW;

-- Describe the stream to see its properties
DESCRIBE STREAM RETAIL_DB.RAW.SALES_CDC_STREAM;

-- ----------------------------------------------------------------------------
-- STEP 3: Check if stream has unprocessed data
-- SYSTEM$STREAM_HAS_DATA returns TRUE if there are pending changes
-- Use this in Task conditions to avoid unnecessary processing
-- ----------------------------------------------------------------------------
SELECT SYSTEM$STREAM_HAS_DATA('RETAIL_DB.RAW.SALES_CDC_STREAM') AS HAS_UNPROCESSED_CHANGES;

-- ----------------------------------------------------------------------------
-- STEP 4: Preview what's currently in the stream
-- (This does NOT consume the stream — only DML operations advance the offset)
-- ----------------------------------------------------------------------------
SELECT
    ORDER_ID,
    ORDER_DATE,
    CUSTOMER_ID,
    PRODUCT,
    SALES,
    QUANTITY,
    PROFIT,
    METADATA$ACTION     AS CDC_ACTION,    -- INSERT or DELETE
    METADATA$ISUPDATE   AS IS_UPDATE,     -- TRUE if part of an UPDATE operation
    METADATA$ROW_ID     AS ROW_ID
FROM RETAIL_DB.RAW.SALES_CDC_STREAM
LIMIT 20;

-- ----------------------------------------------------------------------------
-- STEP 5: Simulate new data arriving (to test CDC)
-- In production, this would be a new file loaded via COPY INTO
-- ----------------------------------------------------------------------------
-- Simulate a new order INSERT
INSERT INTO RETAIL_DB.RAW.RAW_SALES (ORDER_ID, ORDER_DATE, CUSTOMER_ID, CUSTOMER_NAME,
    PRODUCT, CATEGORY, REGION, SALES, QUANTITY, PROFIT)
VALUES
    ('ORD-031', '2024-06-01', 'CUST-120', 'Tina Hall', 'Wireless Charger',
     'Electronics', 'East', 39.99, 2, 12.00),
    ('ORD-032', '2024-06-05', 'CUST-101', 'Alice Johnson', 'Standing Lamp',
     'Furniture', 'North', 89.99, 1, 25.00);

-- Simulate an UPDATE (Snowflake streams capture this as DELETE + INSERT pair)
UPDATE RETAIL_DB.RAW.RAW_SALES
SET SALES = 55.99, PROFIT = 18.00
WHERE ORDER_ID = 'ORD-001';

-- Check stream now shows the changes
SELECT
    ORDER_ID,
    SALES,
    METADATA$ACTION   AS CDC_ACTION,
    METADATA$ISUPDATE AS IS_UPDATE,
    METADATA$ROW_ID
FROM RETAIL_DB.RAW.SALES_CDC_STREAM
ORDER BY ORDER_ID, METADATA$ACTION;

-- ─────────────────────────────────────────────────────────────────────────────
-- EXPLANATION: What you'll see for the UPDATE on ORD-001:
--   Row 1: ORDER_ID=ORD-001, ACTION=DELETE, IS_UPDATE=TRUE  (the old values)
--   Row 2: ORDER_ID=ORD-001, ACTION=INSERT, IS_UPDATE=TRUE  (the new values)
--
-- The MERGE statement in the next step handles this pattern correctly:
--   - WHEN MATCHED AND ACTION=DELETE → DELETE or UPDATE in target
--   - WHEN NOT MATCHED AND ACTION=INSERT → INSERT in target
-- ─────────────────────────────────────────────────────────────────────────────
