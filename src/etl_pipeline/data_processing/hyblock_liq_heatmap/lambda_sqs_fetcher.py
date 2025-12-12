import os
import gzip
import json
import base64
from typing import Any
from pydantic import ValidationError
from botocore.client import ClientError
from datetime import datetime, timedelta
from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source


from data_processing.shared.logger import logger
from data_processing.hyblock_liq_heatmap.client import HyblockClient
from data_processing.hyblock_liq_heatmap.models import FetchEvent, SqsMessage
from data_processing.shared.boto3 import get_sqs_client, get_session


BUFFER_QUEUE_URL = os.environ.get("BUFFER_QUEUE_URL")
SQS_CLIENT = get_sqs_client(get_session())


def generate_minute_timestamps(date_str: str) -> list[str]:
    date = datetime.strptime(date_str, "%Y-%m-%d")
    start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = date.replace(hour=23, minute=59, second=0, microsecond=0)
    timestamps = []
    current_time = start_time
    while current_time <= end_time:
        timestamps.append(int(current_time.timestamp()))
        current_time += timedelta(minutes=1)
    return timestamps


def _gzip_data(
    data: dict[str, Any],
    timestamp: int,
    s3_postfix: str,
) -> SqsMessage:
    original_json = json.dumps(data, separators=(",", ":"))
    json_size = len(original_json.encode("utf-8"))
    compressed = gzip.compress(original_json.encode("utf-8"))
    b64_compressed = base64.b64encode(compressed).decode("utf-8")
    compressed_size = len(b64_compressed.encode("utf-8"))
    logger.info("original json size %s bytes", str(json_size))
    logger.info("compressed response size %s  bytes", str(compressed_size))
    return SqsMessage(
        zip_data=b64_compressed, timestamp=timestamp, s3_postfix=s3_postfix
    )


def _send_zip_data_to_buffer_queue(zip_data: SqsMessage) -> dict[str, Any] | None:
    logger.info("start to send zip data into Buffer queue")
    if BUFFER_QUEUE_URL is None:
        raise ValueError("Buffer Queue cannot be empty!")
    try:
        response = SQS_CLIENT.send_message(
            QueueUrl=BUFFER_QUEUE_URL,
            MessageBody=zip_data.model_dump_json(),
        )
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": f"Successfully send zip data to buffer queue {response['MessageId']}"
                }
            ),
        }
    except ClientError as e:
        logger.error("Boto3 Client Error %s", e)
        raise
    except Exception as e:
        logger.error("Exception %s", e)
        raise


def fetch_handler(
    event: dict[str, Any], _context: dict[str, Any]
) -> dict[str, Any] | None:
    try:
        fetch_event = FetchEvent(**event)
        symbol = fetch_event.symbol
        exchange = fetch_event.exchange
        lookback = fetch_event.lookback
        scaling = fetch_event.scaling
        leverage = fetch_event.leverage
        s3_postfix = f"{symbol}/{exchange}/lookback={lookback}/scaling={scaling}/leverage={leverage}"
        client = HyblockClient()
        start_ts = fetch_event.start_ts
        response = client._get_request(
            "/liquidationHeatmap",
            {
                "coin": fetch_event.symbol,
                "exchange": fetch_event.exchange,
                "lookback": fetch_event.lookback,
                "leverage": fetch_event.leverage,
                "ohlcgraph": fetch_event.exchange,
                "scaling": fetch_event.scaling,
                "timestamp": start_ts,
            },
        )
        if response is None:
            logger.error("No response due to HTTP error")
            return None  # Succeed to delete message like Error>=400 (except 429)
        logger.info("Complete Fetching")
        return _send_zip_data_to_buffer_queue(
            _gzip_data(
                response,
                start_ts,
                s3_postfix,
            )
        )
    except ValidationError as e:
        logger.error("Validation Error %s", e)
        return None
    except Exception as e:
        logger.error("Error processing event: %s", e)
        raise  # Propagate for SQS retry


@event_source(data_class=SQSEvent)
def sqs_consume_handler(event: SQSEvent, _context: Any) -> dict:
    batch_item_failures = []
    for record in event.records:
        try:
            logger.info(record.json_body)
            result = fetch_handler(record.json_body, {})
            if result is None:
                batch_item_failures.append({"itemIdentifier": record.message_id})
        except Exception as e:
            logger.error(f"Error processing record {record.message_id}: {e}")
            batch_item_failures.append({"itemIdentifier": record.message_id})

    return {"batchItemFailures": batch_item_failures}


if __name__ == "__main__":
    event = {
        "symbol": "BTC",
        "exchange": "binance_perp_stable",
        "lookback": "12h",
        "scaling": "max",
        "leverage": "all",
        "start_ts": 1751328000,
    }
    test_result = fetch_handler(event, {})
    if test_result is not None:
        unzip_result = gzip.decompress(base64.b64decode(test_result["zip_data"]))
        print(json.loads(unzip_result))
        print(f"unzip data used bytes {len(unzip_result)} bytes")
