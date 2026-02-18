import argparse
import logging
import requests
from datetime import datetime
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s")

def ingest_github_to_gcs(month: str):
    GITHUB_BASE_URL = (
        "https://raw.githubusercontent.com/"
        "Putriregitas/fmcg_project/main/raw")

    BUCKET_NAME = "fmcg-data-lake"
    GCS_RAW_PATH = "raw"
    INGEST_DATE = datetime.today().strftime("%Y-%m-%d")

    MASTER_FILES = {
        "product": "raw_product_master.csv",
        "outlet": "raw_outlet_master.csv",}

    PERIODIC_FILES = {
        "sales_transaction": "raw_sales_transaction.csv",
        "inventory_outlet_daily": "raw_inventory_outlet_daily.csv",
        "inventory_warehouse_daily": "raw_inventory_warehouse_daily.csv",}

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    #master data
    for dataset, file_name in MASTER_FILES.items():
        url = f"{GITHUB_BASE_URL}/master/{file_name}"
        response = requests.get(url)

        if response.status_code != 200:
            raise RuntimeError(f"Failed to download {file_name}")

        gcs_path = (
            f"{GCS_RAW_PATH}/master/{dataset}/"
            f"ingest_date={INGEST_DATE}/"
            f"{file_name}")

        bucket.blob(gcs_path).upload_from_string(
            response.text,
            content_type="text/csv")

        logging.info(f"Uploaded master {dataset}")

    #periodic data
    for dataset, file_name in PERIODIC_FILES.items():
        url = f"{GITHUB_BASE_URL}/{dataset}/{month}/{file_name}"
        response = requests.get(url)

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to download {file_name} for month {month}")

        gcs_path = (
            f"{GCS_RAW_PATH}/{dataset}/"
            f"period={month}/"
            f"ingest_date={INGEST_DATE}/"
            f"{file_name}")

        bucket.blob(gcs_path).upload_from_string(
            response.text,
            content_type="text/csv")

        logging.info(f"Uploaded {dataset} for period {month}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True)
    args = parser.parse_args()

    ingest_github_to_gcs(args.month)
