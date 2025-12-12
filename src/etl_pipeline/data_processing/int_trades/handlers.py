import os
import json
from typing import Any
from datetime import date
from pydantic import BaseModel, ValidationError
from data_processing.shared.boto3 import get_session, get_s3_client
from data_processing.int_trades.shared import logger

BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
os.environ["POLARS_TEMP_DIR"] = "/tmp"  # noqa: S108
CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks
S3_CLIENT = get_s3_client(get_session())


class TasksGenerationEvent(BaseModel):
    coin_name: str
    start_year: int
    start_month: int
    end_year: int
    end_month: int
    base_s3_prefix: str
    tick_size: float
    price: float


class DownloadUnzipEvent(BaseModel):
    coin_name: str
    zip_url: str
    s3_bucket: str
    s3_zip_path: str


class DataEtlEvent(BaseModel):
    s3_csv_path: str


def _generate_error_response(
    status_code: int, message: str, details: dict | None = None
) -> dict[str, Any]:
    """Generate standardized error response."""
    response = {
        "statusCode": status_code,
        "body": json.dumps({"message": message, **(details or {})}),
    }
    logger.error("%s:%s", message, details or "")
    return response


def generate_tasks_handler(
    event: dict[str, Any], _context: dict[str, Any]
) -> dict[str, Any]:
    """
    input event: {
        coin_name:str
        start_year:int
        start_month:int
        end_year:int
        end_month:int
        base_s3_prefix:str
    }

    Returns:
        dict containing tasks with zip_url, s3_bucket, and s3_prefix
    """
    try:
        etl_event = TasksGenerationEvent(**event)
    except ValidationError as e:
        return _generate_error_response(
            400, "Invalid input parameters", {"errors": e.errors()}
        )

    tasks = []
    current_date = date(etl_event.start_year, etl_event.start_month, 1)
    end_date = date(etl_event.end_year, etl_event.end_month, 1)

    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        month_str = f"{month:02d}"

        zip_url = (
            f"https://data.binance.vision/data/futures/um/monthly/trades/"
            f"{etl_event.coin_name}/{etl_event.coin_name}-trades-{year}-{month_str}.zip"
        )
        zip_path = f"int_trades/{etl_event.coin_name}/raw_data/year={year}/month={month_str}/{etl_event.coin_name}-trades-{year}-{month_str}.zip"

        tasks.append(
            {"zip_url": zip_url, "s3_bucket": BUCKET_NAME, "zip_path": zip_path}
        )

        current_date = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)

    logger.info("Generated %d task for %s", len(tasks), etl_event.coin_name)
    return {"statusCode": 200, "coin_name": event.get("coin_name"), "tasks": tasks}


if __name__ == "__main__":
    event = {
        "coin_name": "BTCUSDT",
        "start_year": 2025,
        "start_month": 1,
        "end_year": 2025,
        "end_month": 6,
        "base_s3_prefix": "raw_data",
    }
    tasks = generate_tasks_handler(event, event)
    print(tasks)
