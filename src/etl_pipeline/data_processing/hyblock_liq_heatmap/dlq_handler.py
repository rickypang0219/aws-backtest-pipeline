import os
from typing import Any, cast

from data_processing.shared.logger import logger
from data_processing.shared.boto3 import get_sqs_client, get_session


SQS_CLIENT = get_sqs_client(get_session())
EVENT_QUEUE_URL = os.environ.get("EVENT_QUEUE_URL")
UNPROCESSED_EVENT_QUEUE_URL = os.environ.get("UNPROCESSED_EVENT_QUEUE_URL")


def put_dead_events_to_event_queue_handler(
    _event: dict[str, Any], _context: dict[str, Any]
) -> None:
    if (
        SQS_CLIENT is None
        or EVENT_QUEUE_URL is None
        or UNPROCESSED_EVENT_QUEUE_URL is None
    ):
        raise ValueError("Invalid SQS Client/Event Queue URL/ DLQ URL")
    try:
        while True:
            resp = SQS_CLIENT.receive_message(
                QueueUrl=UNPROCESSED_EVENT_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1,
                VisibilityTimeout=10,
            )

            msgs = resp.get("Messages", [])
            if not msgs:
                logger.info("No more messages in DLQ.")
                break

            for msg in msgs:
                body = msg.get("Body")
                receipt_handle = msg.get("ReceiptHandle")
                if body is not None:
                    logger.info("Processing %s", cast("str", msg.get("Body")))
                    SQS_CLIENT.send_message(QueueUrl=EVENT_QUEUE_URL, MessageBody=body)
                if receipt_handle is not None:
                    SQS_CLIENT.delete_message(
                        QueueUrl=UNPROCESSED_EVENT_QUEUE_URL,
                        ReceiptHandle=receipt_handle,
                    )
        logger.info("Finished Putting Dead events back to Event Queue")
    except Exception as e:
        logger.error("Exception %s", e)
        raise
