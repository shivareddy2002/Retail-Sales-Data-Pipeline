-- =============================================================================
-- FILE: 01_setup/01_snowflake_environment_setup.sql
-- PROJECT: Retail Sales Data Pipeline
-- DESCRIPTION: Initial environment setup - database, warehouse, schema, roles
-- AUTHOR: Data Engineering Team
-- CREATED: 2024-01-01
-- =============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Use ACCOUNTADMIN role for initial setup
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- STEP 2: Create a dedicated Virtual Warehouse
-- SIZE: X-SMALL is enough for dev/portfolio; scale up in production
-- AUTO_SUSPEND: Suspends after 60 seconds of inactivity → saves costs
-- AUTO_RESUME: Automatically resumes when a query is submitted
-- ----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Retail Sales Data Pipeline';

-- ----------------------------------------------------------------------------
-- STEP 3: Create the main Database
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS RETAIL_DB
    COMMENT = 'Retail Sales Analytics Database';

-- ----------------------------------------------------------------------------
-- STEP 4: Create Schemas (Medallion-style layered architecture)
--   RAW     → Landing zone for ingested CSV data (untouched)
--   STAGING → Cleaned and transformed data
--   MART    → Star schema tables for BI consumption
--   AUDIT   → Data quality logs and pipeline metadata
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS RETAIL_DB.RAW
    COMMENT = 'Raw landing zone - CSV data loaded as-is';

CREATE SCHEMA IF NOT EXISTS RETAIL_DB.STAGING
    COMMENT = 'Cleaned and transformed data layer';

CREATE SCHEMA IF NOT EXISTS RETAIL_DB.MART
    COMMENT = 'Star schema dimensional model for BI';

CREATE SCHEMA IF NOT EXISTS RETAIL_DB.AUDIT
    COMMENT = 'Data quality checks and pipeline run logs';

-- ----------------------------------------------------------------------------
-- STEP 5: Create a dedicated Role and User (production best practice)
-- ----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS RETAIL_ENGINEER_ROLE
    COMMENT = 'Role for data engineering pipeline operations';

-- Grant warehouse and database privileges
GRANT USAGE ON WAREHOUSE RETAIL_WH            TO ROLE RETAIL_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON DATABASE RETAIL_DB    TO ROLE RETAIL_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE RETAIL_DB TO ROLE RETAIL_ENGINEER_ROLE;

-- Grant to SYSADMIN so it inherits (best practice role hierarchy)
GRANT ROLE RETAIL_ENGINEER_ROLE TO ROLE SYSADMIN;

-- ----------------------------------------------------------------------------
-- STEP 6: Set working context for the rest of the project
-- ----------------------------------------------------------------------------
USE ROLE    RETAIL_ENGINEER_ROLE;
USE WAREHOUSE RETAIL_WH;
USE DATABASE  RETAIL_DB;
USE SCHEMA    RETAIL_DB.RAW;

-- Verify setup
SHOW DATABASES LIKE 'RETAIL_DB';
SHOW SCHEMAS   IN DATABASE RETAIL_DB;
SHOW WAREHOUSES LIKE 'RETAIL_WH';
