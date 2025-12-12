import os
import json
import s3fs
import logging
import polars as pl
from typing import Any
from botocore.exceptions import ClientError
from pydantic import BaseModel, ValidationError
from data_processing.shared.boto3 import get_session, get_s3_client


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
s3 = s3fs.S3FileSystem(anon=False)

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME", f"data-bucket-v2-{AWS_ACCOUNT_ID}")
os.environ["POLARS_TEMP_DIR"] = "/tmp"  # noqa: S108
S3_CLIENT = get_s3_client(get_session())


class ConcatEvent(BaseModel):
    coin_name: str


def concat_parquets_handler(
    event: dict[str, Any], _context: dict[str, Any]
) -> dict[str, Any]:
    try:
        concat_event = ConcatEvent(**event)
        coin_name = concat_event.coin_name
        s3_prefix = f"int_trades/{coin_name}/resampled_data/"

        s3_client = S3_CLIENT

        parquet_files = []
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key")
                if key and key.endswith(".parquet"):
                    parquet_files.append(f"s3://{BUCKET_NAME}/{key}")
                else:
                    logger.warning(
                        "Skipping object without valid Key or non-Parquet %s :", obj
                    )
        if not parquet_files:
            logger.warning(
                "No Parquet files found in s3://%s/%s", BUCKET_NAME, s3_prefix
            )
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {"message": "No Parquet files found in the specified path."}
                ),
            }

        logger.info("Found %i  Parquet files: %s", len(parquet_files), parquet_files)

        pl.concat([pl.scan_parquet(file) for file in parquet_files]).sink_parquet(
            f"s3://{BUCKET_NAME}/int_trades/{coin_name}/concated_{coin_name}.parquet"
        )
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Parquet files concatenated successfully."}),
        }
    except ValidationError as e:
        logger.error("Event validation error %s:", e)
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Event Type Error: {e}"}),
        }
    except ClientError as e:
        error_info = e.response.get("Error", {}).get("Code")
        logger.error("AWS ClientError %s:", error_info)
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"AWS Error: {error_info}"}),
        }
    except Exception as e:
        logger.error("Unexpected error %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Unexpected error: {e!s}"}),
        }


if __name__ == "__main__":
    event = {"coin_name": "BTCUSDT"}
    concat_parquets_handler(event, {})
