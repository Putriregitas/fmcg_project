# 📦 FMCG ETL Pipeline
**Sales & Inventory Data Pipeline (Monthly Batch Processing)**

## 📌 Project Overview
Project ini membangun **end-to-end ETL pipeline** untuk data FMCG yang mencakup:
- Sales Transaction
- Inventory Outlet Daily
- Inventory Warehouse Daily
- Product Master
- Outlet Master  

Pipeline dirancang untuk **batch processing bulanan**, dengan struktur data lake yang **versioned** dan **re-runnable**, serta hasil akhir dimuat ke **BigQuery** untuk analisis lanjutan.

---

## 🏗️ High Level Architecture

GitHub (CSV files)  
↓  
Ingestion (Python)  
↓  
Google Cloud Storage (Raw Data Lake)  
↓  
Transformation (Pandas)  
↓  
BigQuery (Analytics Ready Tables)

---

## 📁 Project Structure

```
fmcg_project/
├── ingestion.py          # Ingest data from GitHub to GCS
├── transform.py          # Transform raw data into clean datasets
├── run_pipeline.py       # Main orchestration (Ingest → Transform → Load)
├── README.md
```

---

## 🗂️ Data Lake Structure (GCS)

```
gs://fmcg-data-lake/raw/
├── master/
│   ├── product/
│   │   └── ingest_date=YYYY-MM-DD/
│   └── outlet/
│       └── ingest_date=YYYY-MM-DD/
│
├── sales_transaction/
│   └── period=YYYYMM/
│       └── ingest_date=YYYY-MM-DD/
│
├── inventory_outlet_daily/
│   └── period=YYYYMM/
│       └── ingest_date=YYYY-MM-DD/
│
└── inventory_warehouse_daily/
    └── period=YYYYMM/
        └── ingest_date=YYYY-MM-DD/
```

### 🔑 Penjelasan
- **period (YYYYMM)** → logical partition (bulan data)
- **ingest_date (YYYY-MM-DD)** → physical batch versioning
- Struktur ini memungkinkan:
  - re-run pipeline
  - audit data
  - rollback batch tertentu

---

## 🔄 ETL Flow Detail

### 1️⃣ Ingestion
**Tujuan:**  
Mengambil data CSV dari GitHub dan menyimpannya ke GCS sebagai **raw data lake**.

**Karakteristik:**
- Parameterized by `--month`
- Master data tidak bergantung bulan
- Transactional data dipisah per bulan
- Data tidak ditimpa, hanya bertambah berdasarkan `ingest_date`

---

### 2️⃣ Transformation
**Tujuan:**  
Membersihkan, menstandarkan, dan menyiapkan data untuk analisis.

**Transformasi utama:**
- Normalisasi nama kolom
- Parsing tanggal (`snapshot_date`)
- Channel mapping:
  - General Trade → `GT`
  - Modern Trade → `MT`
- Feature engineering:
  - `flag_oos`
  - `oos_streak`
  - `flag_oos_streak`
  - `flag_data_quality_issue`

**Catatan penting:**
- Parsing tanggal menggunakan format yang konsisten (`YYYY-MM-DD`)
- `errors="coerce"` digunakan untuk menjaga pipeline tetap robust
- Data invalid **tidak dihapus**, hanya ditandai

---

### 3️⃣ Load to BigQuery
**Tujuan:**  
Memuat hasil transform ke BigQuery untuk analisis dan dashboarding.

**Strategi load:**
- `WRITE_TRUNCATE`
- 1 bulan = 1 full refresh
- Cocok untuk data skala kecil–menengah dan project analytics

---

## ▶️ How to Run the Pipeline

### Run Command
```
python run_pipeline.py --month 202401
```

### What Happens
1. Ingest data Januari 2024 dari GitHub → GCS
2. Transform data inventory & sales
3. Load hasil transform ke BigQuery

---

## 🧠 Design Considerations
- Full load per period, incremental antar period
- Master dan transactional data dipisahkan
- Raw data bersifat immutable
- Pipeline modular dan reusable
- Single entry point (`run_pipeline.py`)

---

## 🚀 Future Improvements
- Incremental load ke BigQuery
- Data quality summary table
- Scheduling dengan Airflow / Cloud Composer
- Staging → mart layer separation

---

## 👤 Author
**Putri Regita**  
FMCG Sales & Inventory ETL Project
