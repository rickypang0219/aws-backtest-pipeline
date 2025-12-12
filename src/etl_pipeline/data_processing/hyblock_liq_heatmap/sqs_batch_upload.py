import os
import uuid
import gzip
import json
import s3fs
import base64
import polars as pl
from typing import Any


from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError
from polars.exceptions import ComputeError

from data_processing.shared.logger import logger
from data_processing.shared.boto3 import get_sqs_client, get_session, get_lambda_client


s3 = s3fs.S3FileSystem(anon=False)
AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")
S3_BUCKET = os.environ.get("S3_BUCKET", f"data-bucket-v2-{AWS_ACCOUNT_ID}")
SQS_CLIENT = get_sqs_client(get_session())
LAMBDA_CLIENT = get_lambda_client(get_session())
EVENT_QUEUE_URL = os.environ.get("EVENT_QUEUE_URL")
BUFFER_QUEUE_URL = os.environ.get("BUFFER_QUEUE_URL")
INIT_PIPELINE_ARN = os.environ.get("INIT_PIPELINE_ARN")


def _unzip_data(response: dict[str, Any]) -> dict[str, Any]:
    unzip_response = gzip.decompress(base64.b64decode(response.get("zip_data", "")))
    return json.loads(unzip_response)


def _batch_upload_s3_handler(ldf_list: list[pl.LazyFrame]) -> None:
    if not ldf_list:
        logger.info("No LazyFrames to concatenate, skipping upload")
        return
    concat_ldf = pl.concat(ldf_list)
    for name, sub_df in concat_ldf.collect(engine="streaming").group_by("s3_postfix"):
        parquet_id = uuid.uuid4()
        sub_df.sort("timestamp").write_parquet(
            f"s3://{S3_BUCKET}/hyblock_liq_heatmap/{name[0]}/{parquet_id}.parquet"
        )


def _check_event_queue_emptyness() -> bool:
    if EVENT_QUEUE_URL is None:
        return False
    try:
        response = SQS_CLIENT.get_queue_attributes(
            QueueUrl=EVENT_QUEUE_URL,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )

        if not response or "Attributes" not in response:
            logger.error(
                "Failed to retrieve queue attributes (response is None or malformed)."
            )
            return False
        visible = int(response["Attributes"].get("ApproximateNumberOfMessages", 0))
        inflight = int(
            response["Attributes"].get("ApproximateNumberOfMessagesNotVisible", 0)
        )
        return (visible == 0) and (inflight == 0)
    except (ClientError, EndpointConnectionError, NoCredentialsError) as e:
        logger.error("AWS error: %s", e)
        return False
    except Exception as e:
        logger.error("Execption %s", e)
        return False


def sqs_buffer_queue_consume_handler(
    _event: dict[str, Any], _context: dict[str, Any]
) -> None:
    if SQS_CLIENT is None or BUFFER_QUEUE_URL is None or INIT_PIPELINE_ARN is None:
        raise ValueError("Invalid SQS Client or Buffer Queue URL")

    batch_size = 10  # SQS hard limit
    max_to_process = 1440
    messages_processed = 0

    ldf_list = []
    try:
        while messages_processed < max_to_process:
            resp = SQS_CLIENT.receive_message(
                QueueUrl=BUFFER_QUEUE_URL,
                MaxNumberOfMessages=batch_size,
                WaitTimeSeconds=0,
                VisibilityTimeout=600,
            )

            msgs = resp.get("Messages", "")
            if not msgs:
                break

            for msg in msgs:
                body = msg.get("Body")
                receipt_handle = msg.get("ReceiptHandle")
                if body is not None:
                    body_json = json.loads(body)
                    unzip_data = _unzip_data(body_json)
                    s3_postfix = body_json.get("s3_postfix")
                    ldf = pl.LazyFrame(unzip_data["data"]).with_columns(
                        pl.lit(s3_postfix).alias("s3_postfix")
                    )
                    ldf_list.append(ldf)
                if receipt_handle is not None:
                    SQS_CLIENT.delete_message(
                        QueueUrl=BUFFER_QUEUE_URL,
                        ReceiptHandle=receipt_handle,
                    )
            messages_processed += len(msgs)
        logger.info("Finished Fetching, Start Batch Upload to S3")
        if ldf_list is not None and len(ldf_list) > 0:
            _batch_upload_s3_handler(ldf_list)
            logger.info(
                "Finished Uploading to S3 and init next fetch if event queue is empty"
            )
        if not _check_event_queue_emptyness():
            logger.info("Event Queue is not empty, pause next round fetch until empty")
            return
        logger.info("Event queue is empty, start next fetch")
        LAMBDA_CLIENT.invoke(
            FunctionName=INIT_PIPELINE_ARN,
            InvocationType="Event",
            Payload=json.dumps(
                {
                    "symbol": "BTC",
                    "exchange": "binance_perp_stable",
                    "lookback": "7d",
                    "scaling": "relative",
                    "leverage": "all",
                }
            ),
        )
    except Exception as e:
        logger.error("Exception %s", e)
    except ComputeError as e:
        logger.error("Polars error %s", e)
    except ClientError as e:
        logger.error("S3FS Error %s", e)
