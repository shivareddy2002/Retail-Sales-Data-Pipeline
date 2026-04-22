<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=0:8e2de2,100:4a00e0&height=180&section=header&text=🛒%20Retail%20Sales%20Data%20Pipeline&fontSize=34&fontColor=ffffff&animation=fadeIn&fontAlignY=35"/>
</p>

### End-to-End Data Engineering Project: Incremental Loading, CDC, Star Schema & Power BI

<p align="center">
  <img src="https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white" alt="Snowflake" />
  <img src="https://img.shields.io/badge/SQL-336791?style=for-the-badge&logo=postgresql&logoColor=white" alt="SQL" />
  <img src="https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black" alt="Power BI" />
</p>

---

## 🔥 Project Snapshot

This project demonstrates a **production-grade retail data pipeline** built entirely with modern data stack principles using **Snowflake, SQL, and Power BI**. 

It simulates how modern data engineering teams handle:
- Continuous data ingestion from flat files  
- Change Data Capture (CDC) to track row-level modifications  
- Incremental processing (Upserts) for cost optimization  
- Dimensional Data Modeling (Star Schema) for analytics  
- Business Intelligence reporting  

> ⚡ **From raw CSV → Automated Pipeline → BI Dashboard**

---

## 🧠 Business Use Case

A retail enterprise receives daily sales data from multiple POS (Point of Sale) systems. 
To make agile decisions, the business requires:
- Reliable ingestion of raw, untyped data.
- Efficient processing of *only* changed data (avoiding expensive full-table reloads).
- An optimized, read-heavy schema for reporting.
- Automated dashboards for tracking KPIs like total revenue, regional performance, and product profitability.

---

## 🖼️ Architecture & Visual Workflow

```mermaid
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

```
---

## 🛠️ Tech Stack

| Component       | Technology        | Purpose                          |
| --------------- | ----------------- | -------------------------------- |
| Data Warehouse  | Snowflake         | Storage, compute, processing     |
| Transformation  | SQL               | Data cleaning and transformation |
| Orchestration   | Snowflake Tasks   | Pipeline automation              |
| CDC             | Snowflake Streams | Change tracking                  |
| Visualization   | Power BI          | Dashboards                       |
| Version Control | GitHub            | Code management                  |

---

## 📂 Project Structure

```
retail-data-pipeline/
│
├── 📁 data/
│   └── retail_sales_raw.csv          # Sample dataset (30 orders)
│   └── retail_sales_Dataset.csv      # Kaggle dataset
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
│   └──screenshot 1
│
├── 📁 docs/
│   └── Project Documentation
│
└── README.md                                    # This file
```
---

## ✨ Key Features

| Feature                    | Description                                 |
| -------------------------- | ------------------------------------------- |
| 🔄 **Incremental Loading** | Uses MERGE to process only new/changed data |
| 📡 **CDC with Streams**    | Tracks INSERT, UPDATE, DELETE automatically |
| ⚙️ **Task Automation**     | Scheduled pipelines using Snowflake Tasks   |
| ⭐ **Star Schema**          | Optimized dimensional model for analytics   |
| 🧪 **Data Quality**        | Automated validation checks with logging    |
| ⏱️ **Time Travel**         | Access historical data for recovery         |
| 📊 **Power BI Ready**      | DirectQuery + pre-built DAX measures        |

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

## 🔑 Core Concepts

### 📡 1. Change Data Capture (CDC)
- Snowflake Streams track row-level changes automatically
- Captures:
  - INSERT → new records
  - UPDATE → DELETE + INSERT pair
  - DELETE → removed records
- Enables efficient incremental processing

---

### 🔄 2. Incremental Loading (MERGE)
- Avoids full table reloads
- Processes only changed data

```sql
MERGE INTO stg_sales t
USING sales_cdc_stream s
ON t.order_id = s.order_id

WHEN MATCHED AND s.metadata$action = 'INSERT' THEN UPDATE
WHEN MATCHED AND s.metadata$action = 'DELETE' THEN DELETE
WHEN NOT MATCHED AND s.metadata$action = 'INSERT' THEN INSERT
```

### 3. Star Schema
- **FACT_SALES** — one row per order transaction with measures (sales, profit)
- **DIM_CUSTOMER** — customer attributes + segmentation
- **DIM_PRODUCT** — product name + category
- **DIM_REGION** — region lookup table
- **DIM_DATE** — calendar table with year/month/quarter attributes
### 💡 Why Star Schema?
- Faster query performance
- Simplified joins
- Optimized for BI tools like Power BI
---

**Skills demonstrated:** Snowflake · SQL · ETL · CDC · Star Schema · Dimensional Modeling · Power BI · Pipeline Automation

```markdown
## 🚀 Getting Started

### Prerequisites
- Snowflake Account
- Power BI Desktop
- SnowSQL / Web UI

### Steps

1. Clone the repository  
```bash
git clone https://github.com/shivareddy2002/retail-data-pipeline.git
2. Run SQL scripts in order (01 → 09)
3. Upload dataset to Snowflake Stage
4. Execute pipeline tasks
5. Connect Power BI to Snowflake

--- 

## 🔭 Future Scope

- Implement **Snowpipe** for real-time, event-driven ingestion from AWS S3 / Azure Blob Storage  
- Integrate **dbt (Data Build Tool)** for modular, reusable, and tested data models  
- Use **Apache Airflow** for scalable, cross-platform pipeline orchestration
- 
---

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
