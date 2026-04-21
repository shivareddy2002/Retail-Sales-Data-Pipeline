<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=0:8e2de2,100:4a00e0&height=180&section=header&text=🛒%20Retail%20Sales%20Data%20Pipeline&fontSize=34&fontColor=ffffff&animation=fadeIn&fontAlignY=35"/>
</p>

### *End-to-End Data Engineering Project: Incremental Loading, CDC, Star Schema & Power BI*

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

---

## 🔥 Project Snapshot

This project demonstrates a **production-grade retail data pipeline** built using **Snowflake + SQL + Power BI**.

It simulates how modern data teams handle:
- Continuous data ingestion  
- Change Data Capture (CDC)  
- Incremental processing  
- Data modeling (Star Schema)  
- Business reporting  

> ⚡ **From raw CSV → Automated pipeline → BI Dashboard**

---

## 📌 Project Overview

This project covers the **complete data lifecycle**:

1. Raw data ingestion from CSV  
2. Data cleaning & transformation (ETL)  
3. Change Data Capture using Streams  
4. Incremental loading using MERGE  
5. Automated pipelines using Tasks  
6. Star Schema modeling  
7. Power BI dashboard integration  

---

## 🧠 Business Use Case

A retail company receives daily sales data from POS systems.

They need:
- Reliable ingestion of raw data  
- Efficient processing of only changed data  
- Optimized schema for reporting  
- Automated dashboards for decision-making  

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|--------|
| **Snowflake** | Cloud Data Warehouse |
| **SQL** | Data transformation & querying |
| **Snowflake Streams** | Change Data Capture |
| **Snowflake Tasks** | Automation & scheduling |
| **Power BI Desktop** | Data visualization |
| **CSV** | Source data |
| **Git / GitHub** | Version control |

---

## 📂 Project Structure


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

**Skills demonstrated:** Snowflake · SQL · ETL · CDC · Star Schema · Dimensional Modeling · Power BI · Pipeline Automation

--- 

## 🖼️ Visual Workflow

flowchart LR

%% ===================== DATA INGESTION =====================
subgraph ING[📥 Data Ingestion]
    A["📁 CSV File"]
    B["📦 Snowflake Stage"]
    C["📄 RAW_SALES Table"]
end

%% ===================== CDC =====================
subgraph CDC[📡 Change Data Capture]
    D["🔄 SALES_CDC_STREAM"]
end

%% ===================== TRANSFORMATION =====================
subgraph TR[⚙️ Transformation Layer]
    E["🧹 STG_SALES (Cleaned Data)"]
    F["🔁 MERGE (Incremental Load)"]
end

%% ===================== DATA MODELING =====================
subgraph DM[⭐ Star Schema]
    G["👤 DIM_CUSTOMER"]
    H["📦 DIM_PRODUCT"]
    I["🌍 DIM_REGION"]
    J["📅 DIM_DATE"]
    K["📊 FACT_SALES"]
end

%% ===================== ANALYTICS =====================
subgraph BI[📊 Analytics & Reporting]
    L["📈 Power BI Dashboard"]
end

%% ===================== FLOW =====================
A --> B --> C --> D --> F --> E
E --> G
E --> H
E --> I
E --> J
G --> K
H --> K
I --> K
J --> K
K --> L

%% ===================== STYLING =====================
style A fill:#FFD54F,stroke:#F57F17,color:#000
style B fill:#4FC3F7,stroke:#0277BD,color:#fff
style C fill:#4FC3F7,stroke:#01579B,color:#fff
style D fill:#BA68C8,stroke:#4A148C,color:#fff
style E fill:#AED581,stroke:#33691E,color:#000
style F fill:#FF8A65,stroke:#BF360C,color:#fff
style K fill:#90CAF9,stroke:#0D47A1,color:#000
style L fill:#F44336,stroke:#B71C1C,color:#fff

## 👨‍💻 Author  

**Lomada Siva Gangi Reddy**  
- 🎓 B.Tech CSE (Data Science), RGMCET (2021–2025)  
- 💡 Interests: Python | Machine Learning | Deep Learning | Data Science  
- 📍 Open to **Internships & Job Offers**

 **Contact Me**:  

- 📧 **Email**: lomadasivagangireddy3@gmail.com  
- 📞 **Phone**: 9346493592  
- 💼 [LinkedIn](https://www.linkedin.com/in/lomada-siva-gangi-reddy-a64197280/)  🌐 [GitHub](https://github.com/shivareddy2002)  🚀 [Portfolio](https://lsgr-portfolio-pulse.lovable.app/)

---
<p align="center"> <img src="https://capsule-render.vercel.app/api?type=waving&color=0:8e2de2,100:4a00e0&height=120&section=footer"/> </p>
