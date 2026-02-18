import argparse
from google.cloud import bigquery
from ingestion import ingest_github_to_gcs
from transform import run_transform_pipeline

PROJECT_ID = "halocamp-477806"
DATASET_ID = "fmcg_raw"
BUCKET_NAME = "fmcg-data-lake"

# tabel yang memang harus append (punya histori)
PERIODIC_TABLES = [
    "sales_transaction",
    "inventory_outlet_daily",
    "inventory_warehouse_daily",]

# tabel master (harus overwrite)
MASTER_TABLES = [
    "product_master",
    "outlet_master",]

from google.cloud import bigquery

def load_df_to_bigquery(df, table_name, period=None):

    client = bigquery.Client(project=PROJECT_ID)

    if period:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}${period}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field="period"))

        print(f"Replacing partition {period} in {table_name}")

    else:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE")

        print(f"Replacing full table {table_name}")

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config)

    job.result()
    print(f"Loaded {table_name} successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--month",
        required=True,
        help="Period in YYYYMM format (contoh: 202401)")
    args = parser.parse_args()
    month = args.month
    print(f"START PIPELINE | period={month}")
    ingest_github_to_gcs(month)
    results = run_transform_pipeline(
        BUCKET_NAME,
        month)
    for table_name, df in results.items():
        if table_name in PERIODIC_TABLES:
            load_df_to_bigquery(df, table_name, month)
        else:
            load_df_to_bigquery(df, table_name)
    print("PIPELINE FINISHED SUCCESSFULLY")
