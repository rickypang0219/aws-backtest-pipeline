import logging
from typing import TYPE_CHECKING
from botocore.exceptions import ClientError


if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def generate_month_strings(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> list[str]:
    month_strings = []
    current_year = start_year
    current_month = start_month

    while True:
        month_strings.append(f"{current_year:04d}-{current_month:02d}")
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
        if current_year > end_year or (
            current_year == end_year and current_month > end_month
        ):
            break
    return month_strings


def check_s3_file_exists(
    s3_client: "S3Client",
    bucket_name: str,
    key: str,
) -> bool:
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        logger.error("Client Error%s", e)
        return False
