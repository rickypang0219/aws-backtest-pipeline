import os
import json
import datetime

from botocore.client import ClientError
from typing import Any, cast, TYPE_CHECKING
from collections.abc import Generator
from pydantic import ValidationError
from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef

from data_processing.shared.boto3 import get_sqs_client, get_session, get_lambda_client
from data_processing.shared.logger import logger
from data_processing.hyblock_liq_heatmap.models import FetchEvent, InitEvent

from data_processing.hyblock_liq_heatmap.db import (
    query_timestamp_from_db,
    write_timestamp_to_ddb,
)

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef


SQS_CLIENT = get_sqs_client(get_session())
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")
AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")
EVENT_LAMBDA_ARN = os.environ.get(
    "EVENT_LAMBDA_ARN",
    f"arn:aws:lambda:us-east-1:{AWS_ACCOUNT_ID}:function:"
    "AwsDataEtlStack-HyblockHeatmapCollectionFlowHybloc-8JBWK7n5nBeo",
)

TARGET_EVENTS_PER_RUN = 144  # 1day = 1440 minute
BATCH_SIZE = 10  # match 1440 minutes
END_TIMESTAMP = int(datetime.datetime.strptime("2025-09-01", "%Y-%m-%d").timestamp())


def _generate_event_batches(
    init_event: InitEvent,
    last_timestamp: int,
) -> Generator[list[FetchEvent], None, None]:
    current_timestamp = last_timestamp

    for _batch_count in range(TARGET_EVENTS_PER_RUN):
        batch = []
        for _event_count in range(BATCH_SIZE):
            if current_timestamp <= END_TIMESTAMP:
                batch.append(
                    FetchEvent(
                        symbol=init_event.symbol,
                        exchange=init_event.exchange,
                        leverage=init_event.leverage,
                        lookback=init_event.lookback,
                        scaling=init_event.scaling,
                        start_ts=current_timestamp,
                    )
                )
                current_timestamp += 60
            else:
                break
        if batch:
            yield batch
        if current_timestamp >= END_TIMESTAMP:
            break


def init_pipeline_handler(
    event: dict[str, Any], _context: dict[str, Any]
) -> dict[str, Any]:
    if EVENT_LAMBDA_ARN is None:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Event Generating Lambda ARN is not set!"}),
        }

    try:
        init_event = InitEvent(**event)
        lambda_client = get_lambda_client(get_session())
        last_timestamp = query_timestamp_from_db(init_event)
        if last_timestamp is None or last_timestamp < END_TIMESTAMP:
            lambda_client.invoke(
                FunctionName=EVENT_LAMBDA_ARN,
                InvocationType="Event",
                Payload=json.dumps(event),
            )
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Invoke Next Round Fetching"}),
            }

        return {"statusCode": 200, "body": json.dumps({"message": "Complete Fetching"})}
    except ValidationError as e:
        logger.error("validation error %s", e)
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Event Validation Error"}),
        }


def incremental_events_generator(
    event: dict[str, Any], _context: dict[str, Any]
) -> None:
    try:
        init_event = InitEvent(**event)
        last_timestamp = query_timestamp_from_db(init_event)
        if last_timestamp is None:
            last_timestamp = int(
                datetime.datetime.strptime("2024-03-01", "%Y-%m-%d").timestamp()
            )
        event_batches = _generate_event_batches(init_event, last_timestamp)

        for batch in event_batches:
            entries = [
                {"Id": str(idx), "MessageBody": item.model_dump_json()}
                for idx, item in enumerate(batch)
            ]
            if not SQS_QUEUE_URL:
                raise ValueError("SQS url is not found")
            SQS_CLIENT.send_message_batch(
                QueueUrl=SQS_QUEUE_URL,
                Entries=cast("list[SendMessageBatchRequestEntryTypeDef]", entries),
            )
            last_timestamp = batch[-1].start_ts

        logger.info("Last timestamp %s", str(last_timestamp))
        write_timestamp_to_ddb(init_event, last_timestamp)
    except ValidationError as e:
        logger.error("Event Validation Error %s", e)
    except ClientError as e:
        logger.error("Client Error Failed to upload DDB %s", e)


if __name__ == "__main__":
    from data_processing.hyblock_liq_heatmap.db import write_timestamp_to_ddb

    event = {
        "symbol": "BTC",
        "exchange": "binance_perp_stable",
        "lookback": "7d",
        "scaling": "relative",
        "leverage": "all",
    }

    init_event = InitEvent(**event)  # type: ignore[reportArgumentType]

    print(init_pipeline_handler(event, {}))
