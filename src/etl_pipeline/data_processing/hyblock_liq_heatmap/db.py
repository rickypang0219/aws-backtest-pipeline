import hashlib
from typing import cast, Literal
from boto3.dynamodb.conditions import And, Key
from pydantic import BaseModel

from botocore.exceptions import ClientError
from data_processing.shared.logger import logger
from data_processing.shared.boto3 import get_session
from data_processing.shared.tables import (
    update_ddb,
    write_to_ddb,
    HYBLOCK_STATUS_TABLE,
    query_ddb_with_keys,
)
from data_processing.hyblock_liq_heatmap.models import InitEvent, FetchEvent


class LastProcessTimestamp(BaseModel):
    event_name: str  # Hyblock_LIQ_HEATMAP_TIMESTAMP
    event_id: str  # hash({symbol}#{leverage}#{exchange}#{scaling}#{lookback})
    timestamp: str


class EventStatus(BaseModel):
    event_name: str  # HYBLOCK_LIQ_HEATMAP_EVENT_STATUS
    event_id: (
        str  # hash({symbol}#{leverage}#{exchange}#{scaling}#{lookback}#{timestamp})
    )
    status: Literal["New", "Fail", "Complete"]
    symbol: str
    leverage: str
    exchange: str
    scaling: str
    lookback: str
    timestamp: int


def write_timestamp_to_ddb(event: InitEvent, last_timestamp: int) -> None:
    encoded_bytes = (
        f"{event.exchange}#{event.leverage}#{event.scaling}#{event.lookback}".encode()
    )
    hash_string = hashlib.sha256(encoded_bytes).hexdigest()
    try:
        write_to_ddb(
            get_session(),
            HYBLOCK_STATUS_TABLE,
            [
                {
                    "PK": "HYBLOCK_LIQ_HEATMAP_TIMESTAMP",
                    "SK": hash_string,
                    "timestamp": last_timestamp,
                }
            ],
        )
        logger.info(
            "Upload last processed timestamp %s into DynamoDB",
            str(last_timestamp),
        )
    except ClientError as e:
        logger.error("DynamoDB client error %s", e)
    except Exception as e:
        logger.error("Error info %s", e)


def query_timestamp_from_db(event: InitEvent) -> int | None:
    encoded_bytes = (
        f"{event.exchange}#{event.leverage}#{event.scaling}#{event.lookback}".encode()
    )
    hash_string = hashlib.sha256(encoded_bytes).hexdigest()
    logger.info("query last timestamp in DynmoDB")
    try:
        response = query_ddb_with_keys(
            get_session(),
            HYBLOCK_STATUS_TABLE,
            KeyConditionExpression=And(
                Key("PK").eq(
                    "HYBLOCK_LIQ_HEATMAP_TIMESTAMP",
                ),
                (Key("SK").eq(hash_string)),
            ),
        )
        if not response:
            return None
        print(f"Response information {response}")
        return int(next(iter(response))["timestamp"])
    except ClientError as e:
        logger.error("DynamoDB client error %s", e)
        raise
    except Exception as e:
        logger.error("Error info %s", e)
        raise


def put_events_status_to_db(event_status: list[FetchEvent]) -> None:
    logger.info("Update last timestamp in DynmoDB")
    try:
        status_items = []
        for item in event_status:
            encoded_bytes = (
                f"{item.exchange}#{item.leverage}"
                f"#{item.scaling}#{item.lookback}#{item.start_ts}"
            ).encode()
            hash_string = hashlib.sha256(encoded_bytes).hexdigest()
            status_items.append(
                {
                    "PK": "HYBLOCK_LIQ_HEATMAP_EVENT_STATUS",
                    "SK": hash_string,
                    "status": "New",
                    "symbol": item.symbol,
                    "exchange": item.exchange,
                    "leverage": item.leverage,
                    "lookback": item.lookback,
                    "start_ts": item.start_ts,
                }
            )
        write_to_ddb(
            get_session(),
            HYBLOCK_STATUS_TABLE,
            status_items,
        )
    except ClientError as e:
        logger.error("DynamoDB client error %s", e)
    except Exception as e:
        logger.error("Error info %s", e)


def update_event_status(
    event: FetchEvent, status: Literal["New", "Fail", "Completed"]
) -> None:
    encoded_bytes = (
        f"{event.exchange}#{event.leverage}"
        f"#{event.scaling}#{event.lookback}#{event.start_ts}"
    ).encode()
    hash_string = hashlib.sha256(encoded_bytes).hexdigest()
    try:
        response = update_ddb(
            get_session(),
            HYBLOCK_STATUS_TABLE,
            Key={
                "PK": "HYBLOCK_LIQ_HEATMAP_EVENT_STATUS",
                "SK": hash_string,
            },
            UpdateExpression="SET #status_attr = :new_status",
            ExpressionAttributeNames={"#status_attr": "status"},
            ExpressionAttributeValues={":new_status": status},
            ReturnValues="UPDATED_NEW",
        )
        response = query_ddb_with_keys(
            get_session(),
            HYBLOCK_STATUS_TABLE,
            KeyConditionExpression=And(
                Key("PK").eq(
                    "HYBLOCK_LIQ_HEATMAP_EVENT_STATUS",
                ),
                (Key("SK").eq(hash_string)),
            ),
        )
        print(response)
    except ClientError as e:
        logger.error("DynamoDB client error %s", e)
        raise
    except Exception as e:
        logger.error("Error info %s", e)
        raise


if __name__ == "__main__":
    event = {
        "symbol": "BTC",
        "exchange": "binance_perp_stable",
        "lookback": "12h",
        "scaling": "max",
        "leverage": "all",
        "start_ts": 1751328000,
    }

    fetch_event = cast("FetchEvent", event)
    print(fetch_event)
