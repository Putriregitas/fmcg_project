from google.cloud import bigquery
from ingestion_to_GCS import ingest_github_to_gcs
from transform import run_transform_pipeline

PROJECT_ID = "halocamp-477806"
DATASET_ID = "fmcg_raw"
BUCKET_NAME = "fmcg-data-lake"


def load_df_to_bigquery(df, table_name):
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"))
    job.result()
    print(f"Loaded {table_name}")

if __name__ == "__main__":
    ingest_github_to_gcs()
    results = run_transform_pipeline(BUCKET_NAME)
    for table_name, df in results.items():
        load_df_to_bigquery(df, table_name)