from typing import TYPE_CHECKING
from botocore.exceptions import ClientError

from data_processing.int_trades.shared import logger
from data_processing.int_trades.data_downloader import streaming_data_downloader
from data_processing.int_trades.data_processor import process_csv_data
from data_processing.shared.boto3 import get_s3_client, get_session


if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


def check_s3_file_exists(
    s3_client: "S3Client",
    bucket_name: str,
    key: str,
) -> bool:
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        logger.error("Client Error %s", e)
        logger.info("No file then need to fetch and process")
        return False


if __name__ == "__main__":
    import os
    from data_processing.int_trades.filters import (
        int_price_qty_filters,
        int_price_quote_qty_filters,
    )

    s3_bucket_name = os.environ.get("BUCKET_NAME", "")
    os.environ["POLARS_TEMP_DIR"] = "/tmp"  # noqa: S108
    s3_client = get_s3_client(get_session())

    coin_name = os.environ.get("COIN_NAME")
    zip_url = os.environ.get("ZIP_URL")
    zip_path = os.environ.get("ZIP_PATH")
    tick_size = os.environ.get("TICK_SIZE")
    price = os.environ.get("PRICE")

    if not coin_name or not zip_path or not zip_url or not tick_size or not price:
        raise ValueError(
            "One or more required environment variables (COIN_NAME, ZIP_URL, ZIP_PATH) are missing or invalid!"
        )
    temp_zip_path = zip_path
    parquet_s3_key = temp_zip_path.replace("raw_data", "resampled_data").replace(
        "zip", "parquet"
    )
    resampled_path = temp_zip_path.replace("raw_data", "resampled_data")

    filter_exprs = {
        **int_price_qty_filters,
        **int_price_quote_qty_filters,
    }
    extract_path = temp_zip_path.replace("zip", "csv")

    if not check_s3_file_exists(s3_client, s3_bucket_name, parquet_s3_key):
        streaming_data_downloader(zip_url, zip_path)
        process_csv_data(
            f"{zip_path}",
            extract_path,
            s3_bucket_name,
            filter_exprs,
            float(tick_size),
            float(price),
        )
