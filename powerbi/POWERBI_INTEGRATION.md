# Power BI Integration Guide

## Connecting Power BI Desktop to Snowflake

### Prerequisites
- Power BI Desktop installed ([download here](https://powerbi.microsoft.com/desktop/))
- Snowflake account with `RETAIL_ENGINEER_ROLE` access
- Your Snowflake **account identifier** (e.g., `xy12345.us-east-1`)

---

## Step 1: Install the Snowflake ODBC Driver (optional but recommended)

Download from: https://developers.snowflake.com/odbc/

---

## Step 2: Connect Power BI to Snowflake

1. Open **Power BI Desktop**
2. Click **Home → Get Data → More...**
3. Search for **"Snowflake"** and click **Connect**
4. Fill in the connection dialog:

| Field | Value |
|-------|-------|
| Server | `<your_account>.snowflakecomputing.com` |
| Warehouse | `RETAIL_WH` |
| Database | `RETAIL_DB` |
| Schema | `MART` |
| Role | `RETAIL_ENGINEER_ROLE` |

5. Click **OK** → Authenticate with your Snowflake username/password
6. In the Navigator pane, select these tables:

```
✅ FACT_SALES
✅ DIM_CUSTOMER
✅ DIM_PRODUCT
✅ DIM_REGION
✅ DIM_DATE
```

7. Click **Load** (or **Transform Data** if you want to preview first)

---

## Step 3: Set Up Relationships in Power BI

Go to **Model View** (left sidebar icon) and verify or create these relationships:

```
FACT_SALES[CUSTOMER_SK]  → DIM_CUSTOMER[CUSTOMER_SK]   (Many-to-One)
FACT_SALES[PRODUCT_SK]   → DIM_PRODUCT[PRODUCT_SK]     (Many-to-One)
FACT_SALES[REGION_SK]    → DIM_REGION[REGION_SK]       (Many-to-One)
FACT_SALES[DATE_SK]      → DIM_DATE[DATE_SK]            (Many-to-One)
```

Power BI may auto-detect these from the FK constraints. Verify all 4 relationships exist.

---

## Step 4: Create DAX Measures

In the **Data pane**, right-click `FACT_SALES` → **New Measure** and add:

```dax
// Total Revenue
Total Revenue = SUM(FACT_SALES[SALES_AMOUNT])

// Total Profit
Total Profit = SUM(FACT_SALES[PROFIT_AMOUNT])

// Profit Margin %
Profit Margin % = DIVIDE([Total Profit], [Total Revenue], 0) * 100

// Total Orders
Total Orders = COUNTROWS(FACT_SALES)

// Average Order Value
Avg Order Value = DIVIDE([Total Revenue], [Total Orders], 0)

// Month-over-Month Revenue Growth
MoM Growth % =
VAR CurrentMonthRev = [Total Revenue]
VAR PrevMonthRev = CALCULATE(
    [Total Revenue],
    DATEADD(DIM_DATE[FULL_DATE], -1, MONTH)
)
RETURN
DIVIDE(CurrentMonthRev - PrevMonthRev, PrevMonthRev, 0) * 100
```

---

## Step 5: Dashboard Design

### Page 1: Executive Summary

**KPI Cards (top row):**
- Total Revenue → `[Total Revenue]` (format: currency)
- Total Profit → `[Total Profit]` (format: currency)
- Profit Margin % → `[Profit Margin %]` (format: percentage)
- Total Orders → `[Total Orders]` (format: whole number)

**Bar Chart — Top Products by Revenue:**
- Axis: `DIM_PRODUCT[PRODUCT_NAME]`
- Values: `[Total Revenue]`
- Sort: Descending
- Filter: Top N = 10

**Line Chart — Monthly Sales Trend:**
- X-Axis: `DIM_DATE[YEAR_MONTH]`
- Y-Axis: `[Total Revenue]`, `[Total Profit]`
- Legend: None (two lines on same chart)

---

### Page 2: Regional Analysis

**Map Visual:**
- Location: `DIM_REGION[REGION_NAME]`
- Bubble Size: `[Total Revenue]`
- Color Saturation: `[Profit Margin %]`

**Clustered Bar Chart — Revenue by Region:**
- Y-Axis: `DIM_REGION[REGION_NAME]`
- X-Axis: `[Total Revenue]`, `[Total Profit]`

**Table — Region KPIs:**
- Columns: Region, Total Orders, Revenue, Profit, Profit Margin %
- Conditional formatting on Profit Margin %

---

### Page 3: Product & Category Analysis

**Donut Chart — Revenue by Category:**
- Legend: `DIM_PRODUCT[CATEGORY]`
- Values: `[Total Revenue]`

**Stacked Bar — Category × Quarter:**
- X-Axis: `DIM_DATE[YEAR_QUARTER]`
- Y-Axis: `[Total Revenue]`
- Legend: `DIM_PRODUCT[CATEGORY]`

**Matrix — Product Performance:**
- Rows: `DIM_PRODUCT[CATEGORY]`, `DIM_PRODUCT[PRODUCT_NAME]`
- Values: `[Total Revenue]`, `[Total Profit]`, `[Profit Margin %]`

---

### Page 4: Customer Insights

**Pie Chart — Customer Segments:**
- Legend: `DIM_CUSTOMER[CUSTOMER_SEGMENT]`
- Values: `[Total Revenue]`

**Top Customers Table:**
- Columns: Customer Name, Segment, Total Orders, Total Revenue
- Filter: Top 10 by Revenue

**Scatter Chart — Orders vs Revenue per Customer:**
- X-Axis: `[Total Orders]`
- Y-Axis: `[Total Revenue]`
- Details: `DIM_CUSTOMER[CUSTOMER_NAME]`
- Color: `DIM_CUSTOMER[CUSTOMER_SEGMENT]`

---

## Step 6: Slicers (Filters)

Add these slicers to every page for interactivity:

```
📅 Date Range Slicer    → DIM_DATE[FULL_DATE] (between)
🗂 Category Slicer      → DIM_PRODUCT[CATEGORY] (dropdown)
🌍 Region Slicer        → DIM_REGION[REGION_NAME] (checkbox)
📆 Year Slicer          → DIM_DATE[YEAR_NUMBER] (list)
```

---

## Step 7: Refresh Schedule

To keep your dashboard live:

1. Publish report to **Power BI Service** (powerbi.com)
2. Go to **Dataset → Settings → Scheduled Refresh**
3. Set refresh frequency: **Every hour** or **Daily at 7 AM**
4. Enter your Snowflake credentials in **Data source credentials**

> 💡 **Tip:** Use **DirectQuery** mode (instead of Import) if you want real-time data without scheduled refresh. Note: DirectQuery is slower for large datasets.

---

## Recommended Color Theme

Use this color palette for a professional look:

| Color | Hex | Use |
|-------|-----|-----|
| Primary Blue | `#0066CC` | Main bars, primary metrics |
| Profit Green | `#27AE60` | Profit indicators |
| Loss Red | `#E74C3C` | Negative trends |
| Neutral Gray | `#95A5A6` | Background, secondary |
| Accent Orange | `#F39C12` | Highlights, KPI cards |
