# Retail Data Platform (Enterprise Medallion on Snowflake)

## Project Overview
This project upgrades a basic CSV-to-BI pipeline into an enterprise-ready **Bronze → Silver → Gold** data platform with CDC, incremental MERGE, SCD Type 2, DQ controls, monitoring, and Power BI reporting.

## Architecture (text diagram)
```text
CSV/API/Streaming Simulation
        │
        ▼
BRONZE (raw VARIANT + metadata)
        │
        ▼
STREAMS (CDC offsets)
        │
        ▼
SILVER (clean, typed, deduplicated, late-data aware MERGE)
        │
        ▼
GOLD (Star Schema + aggregates)
        │
        ├── Data Quality checks + quarantine
        ├── Monitoring logs + query performance
        └── Power BI dashboards
```

## Tools & Technologies
- Snowflake (DB, Streams, Tasks, Time Travel)
- SQL (ELT and dimensional modeling)
- Power BI (semantic model + dashboards)
- GitHub (versioned project structure)

## Folder Structure
```text
retail-data-platform/
├── data/
│   └── retail_sales_enterprise_sample.csv
├── sql/
│   ├── setup/
│   ├── bronze/
│   ├── streams/
│   ├── silver/
│   ├── gold/
│   ├── tasks/
│   ├── dq/
│   ├── monitoring/
│   ├── optimization/
│   └── analytics/
├── powerbi/
├── docs/
├── images/
└── README.md
```

## Step-by-Step Setup
1. Run `sql/setup/00_environment_setup.sql`.
2. Upload `data/retail_sales_enterprise_sample.csv` to `@BRONZE.STG_RETAIL_CSV`.
3. Run `sql/bronze/01_ingestion_and_bronze.sql`.
4. Run `sql/streams/01_cdc_streams.sql`.
5. Run `sql/silver/01_silver_transform_and_merge.sql`.
6. Run `sql/gold/01_star_schema_and_gold_marts.sql`.
7. Run DQ/monitoring/optimization scripts.
8. Run task DAG script and resume root task.
9. Connect Power BI using `powerbi/POWERBI_INTEGRATION.md`.

## Enterprise Features
- Medallion architecture (Bronze/Silver/Gold)
- Incremental CDC with Snowflake Streams
- SCD Type 2 in `DIM_CUSTOMER`
- Task DAG orchestration every 5 minutes
- Data Quality framework with quarantine
- Monitoring tables and query performance view
- Performance optimization (cluster keys, search optimization, MV)
- Time Travel ready for data recovery/audits

## Why Star Schema?
Star schema separates facts (measures) from dimensions (descriptive context), giving:
- Faster BI query performance with simpler joins
- Reusable conformed dimensions
- Easier business-friendly analytics modeling

## Analytical Queries Included
- Top products by revenue
- Region-wise performance
- Monthly trends
- Customer segmentation (RFM-lite)

## Screenshots (placeholders)
- `images/pipeline_overview.png`
- `images/powerbi_dashboard.png`

## Real-World Impact
This design mirrors production patterns used in enterprise retail:
- Minimizes cost via incremental loads
- Improves trust with DQ + quarantine
- Enables SLA-driven operations with task automation + monitoring
- Supports executive reporting in near real-time
