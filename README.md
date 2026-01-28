# FMCG ETL Pipeline (Python, GCS, BigQuery)

## Overview
Project ini membangun **ETL pipeline end-to-end** untuk data FMCG menggunakan Python.  
Data diambil dari GitHub (CSV), disimpan ke Google Cloud Storage, ditransformasi menggunakan pandas, lalu dimuat ke BigQuery untuk analisis.

Pipeline dibuat **tanpa notebook**, modular, dan siap dikembangkan ke workflow orchestration seperti **Airflow**.

---

## Tech Stack
- Python
- pandas
- Google Cloud Storage (GCS)
- Google BigQuery

---

## ETL Flow
1. **Extract**  
   Mengambil data CSV dari GitHub dan menyimpannya ke GCS (raw layer) dengan versioning `ingest_date`.

2. **Transform**  
   Membersihkan dan menstandardisasi data:
   - Normalisasi nama kolom
   - Konsistensi kategori (contoh: channel GT / MT)
   - Casting tipe data
   - Pembuatan flag analitik (OOS, data quality issue)

3. **Load**  
   Hasil transform (DataFrame) dimuat langsung ke BigQuery menggunakan `WRITE_TRUNCATE` agar idempotent.

---

## Output BigQuery Tables
- `sales_transaction`
- `inventory_outlet_daily`
- `inventory_warehouse_daily`
- `outlet_master`
- `product_master`

---

## Design Highlights
- Modular function per tabel
- Konsisten naming untuk mencegah duplikasi tabel
- Transform tidak auto-execute saat di-import (aman untuk Airflow)
- Pipeline bisa dijalankan manual maupun terorkestrasi

---

## How to Run
```bash
python run_pipeline.py
