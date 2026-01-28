import logging
import requests
from datetime import datetime
from google.cloud import storage

logging.basicConfig(level=logging.INFO)


def ingest_github_to_gcs():
    GITHUB_BASE_URL = (
        "https://raw.githubusercontent.com/"
        "Putriregitas/fmcg_project/main/raw")

    FILES = [
        "raw_sales_transaction.csv",
        "raw_inventory_outlet_daily.csv",
        "raw_inventory_warehouse_daily.csv",
        "raw_outlet_master.csv",
        "raw_product_master.csv",]

    BUCKET_NAME = "fmcg-data-lake"
    GCS_RAW_PATH = "raw"
    INGEST_DATE = datetime.today().strftime("%Y-%m-%d")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    for file_name in FILES:
        logging.info(f"Processing {file_name}")

        url = f"{GITHUB_BASE_URL}/{file_name}"
        response = requests.get(url)

        if response.status_code != 200:
            raise RuntimeError(f"Failed to download {file_name}")

        if response.text.lstrip().startswith("<!DOCTYPE html"):
            raise ValueError(f"{file_name} is HTML, not CSV")

        dataset_name = file_name.replace("raw_", "").replace(".csv", "")
        gcs_path = (
            f"{GCS_RAW_PATH}/"
            f"{dataset_name}/"
            f"ingest_date={INGEST_DATE}/"
            f"{file_name}")

        blob = bucket.blob(gcs_path)
        blob.upload_from_string(response.text, content_type="text/csv")

        logging.info(f"Uploaded to gs://{BUCKET_NAME}/{gcs_path}")


if __name__ == "__main__":
    ingest_github_to_gcs()
