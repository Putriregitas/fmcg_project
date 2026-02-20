# End-to-End ETL Pipeline: GitHub → GCS → BigQuery (FMCG Data Warehouse)

## 📌 Overview

Project ini membangun pipeline ETL end-to-end untuk industri FMCG dengan
arsitektur modern berbasis cloud.

Pipeline ini menggunakan:

-   GitHub sebagai source data (raw CSV)
-   Google Cloud Storage (GCS) sebagai data lake
-   Python (Pandas) untuk transformasi data
-   BigQuery (Partitioned Tables) sebagai data warehouse

Pipeline ini dirancang untuk production-style data processing dengan
fitur utama:

-   Incremental load per bulan
-   Replace partition untuk mencegah duplicate data
-   Master data overwrite dengan versi terbaru
-   Data quality validation
-   Out‑of‑Stock (OOS) detection dan OOS streak calculation

Hasil akhir pipeline adalah dataset yang clean, partitioned, dan siap
digunakan untuk analisis distribusi, inventory performance, dan sales di
level Outlet & SKU.

------------------------------------------------------------------------

## 🏗 Architecture

    GitHub (Raw CSV)
            │
            ▼
    Google Cloud Storage (Data Lake)
            │
            ▼
    Python (Pandas Transform)
            │
            ▼
    BigQuery (Partitioned Data Warehouse)

Pipeline mengikuti layered architecture:

-   Raw Layer → Google Cloud Storage
-   Transform Layer → Python Pandas
-   Warehouse Layer → BigQuery

------------------------------------------------------------------------

## 📂 Data Lake Structure

Data disimpan di GCS dengan struktur berikut:

    raw/
     ├── master/
     │    ├── outlet_master.csv
     │    └── product_master.csv
     │
     └── periodic/
          ├── sales_transaction/
          │    └── period=YYYYMM/
          │         └── ingest_date=YYYY-MM-DD/
          │
          ├── inventory_outlet/
          │    └── period=YYYYMM/
          │         └── ingest_date=YYYY-MM-DD/
          │
          └── inventory_warehouse/
               └── period=YYYYMM/
                    └── ingest_date=YYYY-MM-DD/

Struktur ini mendukung versioning, audit, dan incremental processing.

------------------------------------------------------------------------

## ⚙️ ETL Pipeline Components

Pipeline terdiri dari tiga tahap utama:

### 1️⃣ Ingestion Layer (`ingestion.py`)

Fungsi:

-   Download raw CSV dari GitHub
-   Upload data ke Google Cloud Storage
-   Memisahkan master dan periodic data
-   Menyimpan data ke data lake

Run:

    python ingestion.py --month 202401

------------------------------------------------------------------------

### 2️⃣ Transform Layer (`transform.py`)

Transformasi menggunakan Pandas:

**Sales Transaction**

-   Standardisasi kolom
-   Normalisasi channel
-   Penambahan kolom period untuk partition

**Inventory Outlet**

-   OOS flag detection
-   OOS streak calculation
-   Data quality flag

**Inventory Warehouse**

-   Warehouse OOS flag
-   Allocation availability flag

**Master Data**

-   Cleaning
-   Standardisasi format

Output berupa DataFrame siap load ke BigQuery.

------------------------------------------------------------------------

### 3️⃣ Load Layer (`run_pipeline.py`)

Memuat data ke BigQuery dengan strategi berikut:

**Periodic Tables**

-   Partition Type: MONTH
-   Partition Column: period
-   Write Mode: WRITE_TRUNCATE per partition

Manfaat:

-   Replace hanya partition terkait
-   Tidak duplicate
-   Mendukung incremental load

**Master Tables**

-   Write Mode: WRITE_TRUNCATE full table

------------------------------------------------------------------------

## ▶️ Run Full Pipeline

    python run_pipeline.py --month 202401

Execution flow:

1.  Ingest data ke GCS
2.  Transform data dengan Pandas
3.  Load ke BigQuery

------------------------------------------------------------------------

## 🗄 BigQuery Tables

Dataset:

    fmcg_raw

Tables:

-   sales_transaction (partitioned)
-   inventory_outlet_daily (partitioned)
-   inventory_warehouse_daily (partitioned)
-   outlet_master
-   product_master

------------------------------------------------------------------------

## 🚀 Key Capabilities

-   End-to-end ETL pipeline
-   Incremental monthly processing
-   Partition replacement strategy
-   Data lake architecture
-   Automated data warehouse loading
-   OOS detection logic
-   Analysis-ready dataset

------------------------------------------------------------------------

## 🧰 Tech Stack

-   Python
-   Pandas
-   Google Cloud Storage
-   BigQuery
-   pandas-gbq
-   Requests

------------------------------------------------------------------------

## 📊 Use Case

Dataset dapat digunakan untuk:

-   OOS analysis
-   Inventory performance monitoring
-   Distribution efficiency analysis
-   Sales analysis per outlet dan SKU

------------------------------------------------------------------------

## 📌 Summary

Pipeline ini membangun scalable, clean, dan production-style data
warehouse menggunakan arsitektur:

GitHub → GCS → Pandas → BigQuery
