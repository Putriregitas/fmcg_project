from google.cloud import bigquery, storage


def get_latest_ingest_date(bucket_name: str, dataset_name: str) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    prefix = f"raw/{dataset_name}/ingest_date="
    dates = {
        blob.name.split("ingest_date=")[1].split("/")[0]
        for blob in bucket.list_blobs(prefix=prefix)}

    if not dates:
        raise ValueError("No ingest_date found")

    return max(dates)


def load_latest_to_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    bucket_name: str):
    latest_date = get_latest_ingest_date(
        bucket_name, table_id)

    gcs_uri = (
        f"gs://{bucket_name}/raw/"
        f"{table_id}/"
        f"ingest_date={latest_date}/"
        f"raw_{table_id}.csv")

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"))

    job.result()
