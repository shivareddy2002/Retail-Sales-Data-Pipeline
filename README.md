# 🛒 Retail Sales Data Pipeline

### *End-to-End Data Engineering Project: Incremental Loading, CDC, Star Schema & Power BI*

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
---

## 📌 Project Overview

This project demonstrates a **production-grade retail sales data pipeline** built entirely on **Snowflake**. It covers the full lifecycle of data — from raw CSV ingestion through automated transformation, Change Data Capture (CDC), Star Schema modeling, and final Power BI dashboards.

**Business Use Case:**
A retail company receives daily order files from its point-of-sale system. The business needs:
- A reliable pipeline to ingest and clean raw sales data
- Incremental loading that only processes *new and changed* records
- A dimensional model (Star Schema) that BI tools can query efficiently
- Automated Power BI dashboards showing revenue, profit, and customer trends

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     RETAIL SALES DATA PIPELINE                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  📁 CSV File                                                          │
│      │                                                               │
│      ▼                                                               │
│  [Internal Stage] ──COPY INTO──▶ [RAW_SALES]                         │
│      (Snowflake Stage)              (Raw Layer)                       │
│                                         │                             │
│                                         │ Stream (CDC)                │
│                                         ▼                             │
│                                   [SALES_CDC_STREAM]                  │
│                                         │                             │
│                                         │ MERGE (every 5 min)         │
│                                         ▼                             │
│                                   [STG_SALES]                         │
│                                  (Staging Layer)                      │
│                                         │                             │
│                              ┌──────────┼──────────┐                  │
│                              ▼          ▼          ▼                  │
│                        DIM_CUSTOMER  DIM_PRODUCT  DIM_REGION          │
│                              │          │          │                  │
│                              └──────────┼──────────┘                  │
│                                         │                             │
│                                    [FACT_SALES]                       │
│                                   (MART Layer)                        │
│                                         │                             │
│                                         ▼                             │
│                                  [Power BI Dashboard]                 │
│                                                                       │
│  ⚙️  Snowflake Tasks automate the entire pipeline (every 5 min)       │
│  🔍  DQ Checks log results to AUDIT.DQ_CHECK_LOG                      │
│  ⏱️  Time Travel enables point-in-time recovery                        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| **Snowflake** | Cloud Data Warehouse — storage, compute, streams, tasks |
| **SQL** | All ETL logic, transformations, MERGE statements |
| **Snowflake Streams** | Change Data Capture (CDC) |
| **Snowflake Tasks** | Pipeline automation & scheduling |
| **Power BI Desktop** | Business Intelligence dashboards |
| **CSV** | Source data format |
| **Git / GitHub** | Version control |

---

## 📂 Project Structure

```
retail-data-pipeline/
│
├── 📁 data/
│   └── retail_sales_raw.csv          # Sample dataset (30 orders)
│
├── 📁 sql/
│   ├── 01_setup/
│   │   └── 01_snowflake_environment_setup.sql   # DB, schema, warehouse, roles
│   │
│   ├── 02_raw_layer/
│   │   └── 01_raw_table_and_stage.sql           # Raw table, stage, COPY INTO
│   │
│   ├── 03_transformation/
│   │   └── 01_transform_sales.sql               # Cleaning, typing, derived columns
│   │
│   ├── 04_cdc_streams/
│   │   └── 01_cdc_stream_setup.sql              # Snowflake Stream for CDC
│   │
│   ├── 05_merge_incremental/
│   │   └── 01_incremental_merge.sql             # MERGE statement (upsert)
│   │
│   ├── 06_tasks/
│   │   └── 01_pipeline_tasks.sql                # Automated scheduling
│   │
│   ├── 07_star_schema/
│   │   └── 01_star_schema_design.sql            # Fact + 4 Dimension tables
│   │
│   ├── 08_analytical_queries/
│   │   └── 01_business_analytics.sql            # BI-ready SQL queries
│   │
│   └── 09_data_quality/
│       └── 01_data_quality_and_bonus.sql        # DQ checks, Time Travel, optimization
│
├── 📁 powerbi/
│   └── POWERBI_INTEGRATION.md                   # Step-by-step Power BI guide
│
├── 📁 images/
│   └── (add screenshots here)
│
├── 📁 docs/
│   └── (additional documentation)
│
└── README.md                                    # This file
```

---

## 🚀 Getting Started

### Prerequisites
- Snowflake account (free trial: [signup here](https://signup.snowflake.com/))
- Power BI Desktop (free: [download here](https://powerbi.microsoft.com/desktop/))
- Basic SQL knowledge

### Step-by-Step Setup

**Step 1: Clone this repository**
```bash
git clone https://github.com/yourusername/retail-data-pipeline.git
cd retail-data-pipeline
```

**Step 2: Run environment setup**
```sql
-- In Snowflake UI (Worksheets) or SnowSQL
-- Execute: sql/01_setup/01_snowflake_environment_setup.sql
```

**Step 3: Create raw table and upload CSV**
```sql
-- Execute: sql/02_raw_layer/01_raw_table_and_stage.sql
-- Then upload data/retail_sales_raw.csv to the stage:
PUT file:///path/to/retail_sales_raw.csv @RETAIL_DB.RAW.RETAIL_STAGE;
```

**Step 4: Run the initial transformation**
```sql
-- Execute: sql/03_transformation/01_transform_sales.sql
```

**Step 5: Set up CDC Stream**
```sql
-- Execute: sql/04_cdc_streams/01_cdc_stream_setup.sql
```

**Step 6: Run the initial MERGE**
```sql
-- Execute: sql/05_merge_incremental/01_incremental_merge.sql
```

**Step 7: Activate automated Tasks**
```sql
-- Execute: sql/06_tasks/01_pipeline_tasks.sql
```

**Step 8: Build the Star Schema**
```sql
-- Execute: sql/07_star_schema/01_star_schema_design.sql
```

**Step 9: Validate with analytical queries**
```sql
-- Execute: sql/08_analytical_queries/01_business_analytics.sql
```

**Step 10: Connect Power BI**
```
Follow: powerbi/POWERBI_INTEGRATION.md
```

---

## ✨ Key Features

| Feature | Description |
|---------|-------------|
| 🔄 **Incremental Loading** | Only processes new/changed records — no full table reloads |
| 📡 **CDC with Streams** | Captures INSERT, UPDATE, DELETE changes automatically |
| ⚙️ **Task Automation** | Pipeline runs every 5 minutes without manual intervention |
| ⭐ **Star Schema** | Kimball-style dimensional model for fast BI queries |
| 🧪 **Data Quality** | 5 automated checks with pass/fail logging to audit table |
| ⏱️ **Time Travel** | Query historical data snapshots for auditing and recovery |
| 📊 **Power BI Ready** | Pre-built DAX measures and dashboard design guide |
| 🏷️ **Metadata Tracking** | Every record tagged with load timestamp, source, and version |

---

## 📊 Dataset

The project uses a realistic retail sales dataset with 30 orders across 5 months (Jan–May 2024):

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | VARCHAR | Unique order identifier |
| `order_date` | DATE | Date the order was placed |
| `customer_id` | VARCHAR | Customer identifier |
| `customer_name` | VARCHAR | Full name of the customer |
| `product` | VARCHAR | Product name |
| `category` | VARCHAR | Product category (Electronics, Furniture, etc.) |
| `region` | VARCHAR | Sales region (North/South/East/West) |
| `sales` | FLOAT | Total sales amount (USD) |
| `quantity` | INT | Units sold |
| `profit` | FLOAT | Profit on the order (USD) |

---

## 🔑 Core Concepts Demonstrated

### 1. Change Data Capture (CDC)
Snowflake Streams track row-level changes. When a new file is loaded:
- New orders → INSERT action in stream → inserted into staging
- Updated orders → DELETE + INSERT pair → staging row updated
- Deleted orders → DELETE action → removed from staging

### 2. MERGE Statement
The MERGE upsert handles all three CDC scenarios in a single atomic operation:
```sql
MERGE INTO staging USING stream_data
  WHEN MATCHED AND action = 'INSERT' → UPDATE
  WHEN MATCHED AND action = 'DELETE' → DELETE
  WHEN NOT MATCHED AND action = 'INSERT' → INSERT
```

### 3. Star Schema
- **FACT_SALES** — one row per order transaction with measures (sales, profit)
- **DIM_CUSTOMER** — customer attributes + segmentation
- **DIM_PRODUCT** — product name + category
- **DIM_REGION** — region lookup table
- **DIM_DATE** — calendar table with year/month/quarter attributes

---

## 📸 Screenshots

> Add screenshots of your Snowflake worksheets and Power BI dashboard here.

```
images/
├── snowflake_raw_table.png
├── snowflake_star_schema.png
├── powerbi_dashboard_page1.png
├── powerbi_dashboard_page2.png
└── powerbi_dashboard_page3.png
```

---

## 🤝 Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

## 👤 Author

Built as a portfolio project for Data Engineering roles.

**Skills demonstrated:** Snowflake · SQL · ETL · CDC · Star Schema · Dimensional Modeling · Power BI · Pipeline Automation

---

*⭐ Star this repo if it helped you learn or land a job!*
