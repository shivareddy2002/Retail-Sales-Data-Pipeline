# Retail Sales Data Pipeline with Incremental Loading, CDC, and BI Dashboard

## 1) PROJECT OVERVIEW

### Simple Explanation
This project builds a modern data pipeline where sales CSV files are loaded into Snowflake, cleaned in a staging layer, incrementally processed using CDC (Streams + MERGE), modeled into a star schema, and finally consumed in Power BI dashboards.

### Business Use Case
Retail teams need near-real-time visibility into sales, profit, product performance, and regional trends. Full reloads are costly; this solution processes only changed records and automates refreshes every 5 minutes.

### Architecture
CSV → Snowflake Stage → RAW_SALES → STREAM (CDC) → STG_SALES → MERGE → STAR SCHEMA (FACT + DIMS) → POWER BI

---

## 2) DATASET (Sample 30 Rows)
Source file: `data/retail_sales_raw.csv`

Columns:
- `order_id`
- `order_date`
- `customer_id`
- `customer_name`
- `product`
- `category`
- `region`
- `sales`
- `quantity`
- `profit`

---

## 3) SNOWFLAKE SETUP (STEP-BY-STEP SQL)
Run in order:
1. `sql/01_setup/01_snowflake_environment_setup.sql`
2. `sql/02_raw_layer/01_raw_table_and_stage.sql`

Includes:
- Database, warehouse, schemas, role setup
- Raw landing table
- Internal stage + CSV format
- `COPY INTO` for ingestion

---

## 4) TRANSFORMATION LAYER
Run: `sql/03_transformation/01_transform_sales.sql`

Implements:
- NULL handling via `COALESCE`
- Data type conversion (`TRY_TO_DATE`, `TRY_TO_DECIMAL`, `TRY_TO_NUMBER`)
- Derived columns:
  - `order_month`
  - `order_year`
  - `profit_ratio` = `profit / sales`

---

## 5) STREAM (CDC IMPLEMENTATION)
Run: `sql/04_cdc_streams/01_cdc_stream_setup.sql`

What CDC does here:
- Tracks row-level changes on `RAW_SALES`.
- New rows, updates, and deletes are exposed by the stream.
- Stream offsets advance only when consumed in DML transactions.

---

## 6) MERGE LOGIC (INCREMENTAL LOAD)
Run: `sql/05_merge_incremental/01_incremental_merge.sql`

Pattern used:
- `WHEN MATCHED` → update changed rows
- `WHEN NOT MATCHED` → insert new rows

This avoids full-table reload and minimizes compute cost.

---

## 7) TASK AUTOMATION
Run: `sql/06_tasks/01_pipeline_tasks.sql`

Includes:
- Snowflake task scheduled every 5 minutes (CRON)
- Stream check condition (`SYSTEM$STREAM_HAS_DATA`)
- `ALTER TASK ... RESUME` to activate

Workflow:
1. New file loaded into RAW
2. Stream captures change
3. Task runs MERGE logic
4. Staging and marts stay fresh automatically

---

## 8) STAR SCHEMA DESIGN
Run: `sql/07_star_schema/01_star_schema_design.sql`

### Fact
- `fact_sales`

### Dimensions
- `dim_customer`
- `dim_product`
- `dim_region`
- `dim_date`

Why Star Schema:
- Faster BI query performance
- Simpler joins for analysts
- Better separation between descriptive attributes and measures

---

## 9) ANALYTICAL QUERIES
Run: `sql/08_analytical_queries/01_business_analytics.sql`

Included query groups:
- Top products by revenue
- Region-wise profit
- Monthly sales trends
- Customer segmentation (high/medium/low value)

---

## 10) POWER BI INTEGRATION
See: `powerbi/POWERBI_INTEGRATION.md`

Steps:
1. Open Power BI Desktop → Get Data → Snowflake
2. Enter account URL, warehouse, database, schema
3. Import:
   - `fact_sales`
   - `dim_customer`
   - `dim_product`
   - `dim_region`
   - `dim_date`
4. Build visuals:
   - KPI cards (Revenue, Profit, Orders)
   - Bar chart (Top Products)
   - Line chart (Monthly Sales Trend)
   - Map (Region Revenue)

---

## 11) GITHUB PROJECT STRUCTURE

```text
retail-data-pipeline/
├── data/
├── sql/
├── powerbi/
├── images/
├── docs/
└── README.md
```

---

## 12) README CHECKLIST
Current README includes:
- Project description
- Architecture flow diagram
- Tools used
- Setup steps
- Key features
- Screenshots placeholder section

---

## 13) BONUS (ADVANCED FEATURES)
Implemented in `sql/09_data_quality/01_data_quality_and_bonus.sql`:
1. Data quality checks (nulls, duplicates, range validations)
2. Time Travel examples for recovery/audit
3. Query optimization notes and clustering guidance
4. Warehouse scaling strategy (dev vs prod sizing)

---

## Execution Order
Run SQL scripts exactly in folder-number order (`01` to `09`).

## Portfolio Tip
After executing:
- Capture screenshots from Snowflake and Power BI into `images/`
- Update README image links
- Publish GitHub repo and share with short architecture summary on LinkedIn
