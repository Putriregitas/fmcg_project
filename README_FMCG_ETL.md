# FMCG Sales & Distribution ETL Pipeline

## Project Overview

This project builds an end-to-end ETL pipeline to integrate FMCG sales,
inventory, and master data into a centralized BigQuery data warehouse.

The goal is to create a clean, partitioned, and analysis-ready dataset
at the Outlet & SKU level to support distribution and stock allocation
analysis.

Focus of this repository: **Data Engineering & ETL Implementation**.

------------------------------------------------------------------------

# Architecture

GitHub (Raw CSV)\
→ Google Cloud Storage (Data Lake)\
→ Pandas Transformations\
→ BigQuery (Partitioned Data Warehouse)

------------------------------------------------------------------------

# ETL Pipeline Explanation

## 1️⃣ Ingestion Layer

**File:** `ingestion.py`

-   Downloads raw CSV files from GitHub
-   Uploads data to Google Cloud Storage
-   Separates master and periodic datasets
-   Creates structured data lake paths:

```{=html}
<!-- -->
```
    raw/
     ├── master/
     └── <dataset>/period=YYYYMM/ingest_date=YYYY-MM-DD/

Run ingestion:

    python ingestion.py --month 202401

------------------------------------------------------------------------

## 2️⃣ Transform Layer

**File:** `transform.py`

Data is processed using Pandas:

### Sales Transaction

-   Column standardization
-   Channel normalization (GT / MT)
-   Period column added (DATE type for partitioning)

### Inventory Outlet

-   OOS (Out of Stock) flag detection
-   OOS streak calculation
-   Data quality flags

### Inventory Warehouse

-   Warehouse OOS flag
-   Allocation availability flag

### Master Data

-   Cleaning and formatting
-   Channel normalization

Transform output is returned as DataFrames ready for BigQuery.

------------------------------------------------------------------------

## 3️⃣ Load Layer

**File:** `run_pipeline.py`

BigQuery load strategy:

### 🔥 Partitioned Tables (Periodic Data)

-   Time Partitioning: MONTH
-   Partition Field: `period` (DATE)
-   Write Mode: `WRITE_TRUNCATE` per partition

This ensures: - Reloading the same month replaces existing data - No
duplicates - Other months remain intact

### Master Tables

-   Full table overwrite (`WRITE_TRUNCATE`)

------------------------------------------------------------------------

# Run Full Pipeline

    python run_pipeline.py --month 202401

Pipeline steps: 1. Ingest data to GCS 2. Transform with Pandas 3. Load
into BigQuery (partition replace logic)

------------------------------------------------------------------------

# BigQuery Dataset

Dataset: `fmcg_raw`

Tables: - sales_transaction (Partitioned) - inventory_outlet_daily
(Partitioned) - inventory_warehouse_daily (Partitioned) -
outlet_master - product_master

------------------------------------------------------------------------

# Key Engineering Design

-   Idempotent load (replace partition, not append)
-   Data lake layer before warehouse
-   Separation of ingestion, transform, load
-   Partitioning for performance & scalability

------------------------------------------------------------------------

# Tech Stack

-   Python
-   Pandas
-   Google Cloud Storage
-   Google BigQuery
-   pandas-gbq
-   Requests
