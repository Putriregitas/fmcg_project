import pandas as pd
from google.cloud import storage
from io import BytesIO
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)


#read
def get_latest_ingest_date(bucket_name: str, dataset_name: str) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"raw/{dataset_name}/ingest_date="

    dates = {
        blob.name.split("ingest_date=")[1].split("/")[0]
        for blob in bucket.list_blobs(prefix=prefix)}

    if not dates:
        raise ValueError(f"No ingest_date found for {dataset_name}")

    return max(dates)


def read_raw_from_gcs(
    bucket_name: str,
    dataset_name: str,
    ingest_date: str | None = None) -> pd.DataFrame:

    if ingest_date is None:
        ingest_date = get_latest_ingest_date(
            bucket_name, dataset_name)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    file_name = f"raw_{dataset_name}.csv"
    gcs_path = (
        f"raw/{dataset_name}/"
        f"ingest_date={ingest_date}/"
        f"{file_name}")

    blob = bucket.blob(gcs_path)
    data = blob.download_as_bytes()

    if data.startswith(b"<!DOCTYPE html"):
        raise ValueError(f"{dataset_name} is HTML, not CSV")

    # detect delimiter
    sample = data[:1024].decode("utf-8")
    sep = ";" if ";" in sample and "," not in sample else ","

    df = pd.read_csv(
        BytesIO(data),
        sep=sep)

    logging.info(
        f"Loaded {dataset_name} "
        f"(ingest_date={ingest_date}, sep='{sep}', rows={len(df)})")

    return df



#transform
def transform_product_master(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()

    df = df.rename(columns={
        "SKU_CODE": "sku_id",
        "Brand": "brand",
        "PackSize_ml": "pack_size_ml"})

    if "category" in df.columns:
        df["category"] = (
            df["category"]
            .astype(str)
            .str.capitalize())

    return df


def transform_outlet_master(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()

    df = df.rename(columns={
        "OutletName": "outlet_name",
        "CHANNEL": "channel",
        "Region": "region"})

    df["outlet_name"] = df["outlet_name"].str.title()
    df["city_name"] = df["city_name"].str.title()
    df["region"] = df["region"].str.title()

    channel_map = {
        "gt": "GT",
        "modern trade": "MT"}

    df["channel"] = (
        df["channel"]
        .str.lower()
        .map(channel_map)
        .fillna(df["channel"]))

    return df


def transform_inventory_outlet_daily(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    df = df.rename(columns={"sku_code": "sku_id"})
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df = df.sort_values(
        ["outlet_id", "sku_id", "snapshot_date"])

    df["stock_in"] = df["stock_in"].fillna(0)
    df["sales_visit_flag"] = df["end_stock"].notna()
    df["flag_oos"] = (
        (df["end_stock"] == 0) &
        (df["sales_visit_flag"]))

    df["prev_end_stock"] = (
        df.groupby(["outlet_id", "sku_id"])["end_stock"]
        .shift(1))

    df["flag_data_quality_issue"] = (
        (df["end_stock"] > df["prev_end_stock"]) &
        (df["stock_in"] == 0))

    df = df.drop(columns=["prev_end_stock"])
    return df


def transform_inventory_warehouse_daily(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    df = df.rename(columns={
        "snapshotdate": "snapshot_date",
        "sku_code": "sku_id",
        "endingstock": "ending_stock",
        "warehouse": "warehouse_id"})

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df["flag_oos_wh"] = df["ending_stock"] == 0
    df["flag_available_allocation"] = df["ending_stock"] > 0

    return df


def transform_sales_transaction(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    df = df.rename(columns={
        "sku": "sku_id",
        "sku_code": "sku_id",
        "outlet": "outlet_id",
        "qtysold": "qty_sold"})

    df["channel"] = (
        df["channel"]
        .astype(str)
        .str.lower()
        .replace({
            "gt": "GT",
            "general trade": "GT",
            "mt": "MT",
            "modern trade": "MT"}))

    return df


#orchestration function
def run_transform_pipeline(bucket_name: str):
    df_sales = read_raw_from_gcs(
        bucket_name, "sales_transaction")
    df_inventory_outlet = read_raw_from_gcs(
        bucket_name, "inventory_outlet_daily")
    df_inventory_warehouse = read_raw_from_gcs(
        bucket_name, "inventory_warehouse_daily")
    df_outlet = read_raw_from_gcs(
        bucket_name, "outlet_master")
    df_product = read_raw_from_gcs(
        bucket_name, "product_master")

    return {
    "sales_transaction": transform_sales_transaction(df_sales),
    "inventory_outlet_daily": transform_inventory_outlet_daily(df_inventory_outlet),
    "inventory_warehouse_daily": transform_inventory_warehouse_daily(df_inventory_warehouse),
    "outlet_master": transform_outlet_master(df_outlet),
    "product_master": transform_product_master(df_product),}

if __name__ == "__main__":
    BUCKET_NAME = "fmcg-data-lake"
    run_transform_pipeline(BUCKET_NAME)
