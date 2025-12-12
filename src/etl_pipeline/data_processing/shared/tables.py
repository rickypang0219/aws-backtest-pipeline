import typing
from collections.abc import Iterator
from decimal import Decimal
from typing import Any, cast

import simplejson
from boto3.dynamodb.conditions import ConditionBase, ConditionExpressionBuilder
from boto3.dynamodb.types import Binary, TypeDeserializer, TypeSerializer
from boto3.session import Session

from data_processing.shared.boto3 import DDB_BOTO3_CONFIG, get_ddb_client
from data_processing.shared.cache import lru_cache_when_not_in_test
from data_processing.shared.logger import logger

if typing.TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table


HYBLOCK_STATUS_TABLE = "HyblockStatusTable"


@lru_cache_when_not_in_test()  # caching resource in multi-thread test behaves weirdly
def _get_table_resource(session: Session, table_name: str) -> "Table":
    logger.info("Create new table resource %s", table_name)
    return session.resource("dynamodb", config=DDB_BOTO3_CONFIG).Table(table_name)


def write_to_ddb(
    session: Session,
    table_name: str,
    items: list[dict[str, Any]],
    overwrite_by_pkeys: list[str] | None = None,
) -> None:
    """
    Put items to table
    """
    table = _get_table_resource(session, table_name)
    with (
        table.batch_writer(overwrite_by_pkeys=overwrite_by_pkeys)
        if overwrite_by_pkeys
        else table.batch_writer()
    ) as batch:
        for item in items:
            batch.put_item(Item=prepare_item(item))


def delete_from_ddb(
    session: Session, table_name: str, key: dict[str, Any] | list[dict[str, Any]]
) -> None:
    keys = key if isinstance(key, list) else [key]
    logger.info("Deleting %d items", len(keys))
    table = _get_table_resource(session, table_name)
    with table.batch_writer() as batch:
        for _key in keys:
            batch.delete_item(Key=_key)


def query_ddb_with_keys(
    session: Session, table_name: str, **kwargs: Any
) -> list[dict[str, Any]]:
    table = _get_table_resource(session, table_name)
    return cast("list[dict[str, Any]]", _unfold_data(table.query(**kwargs)["Items"]))


def query_ddb_with_keys_all(
    session: Session,
    table_name: str,
    # Using camel case here to make it consistent with query_ddb_with_keys.
    KeyConditionExpression: ConditionBase,
    IndexName: str | None = None,
) -> Iterator[dict[str, Any]]:
    """
    Query ddb with key condition expression, returning all pages.

    Only support KeyConditionExpression for now.
    """
    serializer = TypeSerializer()
    key_condition = ConditionExpressionBuilder().build_expression(
        KeyConditionExpression, is_key_condition=True
    )
    paginator = get_ddb_client(session).get_paginator("query")
    # The type hints provided by boto3 are not correct, disabling
    kwargs = {
        "TableName": table_name,
        "KeyConditionExpression": key_condition.condition_expression,
        "ExpressionAttributeNames": key_condition.attribute_name_placeholders,
        "ExpressionAttributeValues": {
            k: serializer.serialize(v)
            for k, v in key_condition.attribute_value_placeholders.items()
        },
    }
    if IndexName:
        kwargs["IndexName"] = IndexName
    for response in paginator.paginate(**kwargs):  # type: ignore
        for item in response["Items"]:
            yield {k: deserializer.deserialize(v) for k, v in item.items()}


def get_ddb(
    session: Session, table_name: str, Key: dict[str, Any], **kwargs: Any
) -> dict[str, Any] | None:
    table = _get_table_resource(session, table_name)
    item = table.get_item(Key=Key, **kwargs).get("Item")
    if not item:
        return None
    return cast("dict[str, Any]", _unfold_data(item))


def update_ddb(session: Session, table_name: str, **kwargs: Any) -> None:
    table = _get_table_resource(session, table_name)
    table.update_item(**kwargs)


def update_ddb_and_return_new_item(
    session: Session, table_name: str, **kwargs: Any
) -> dict[str, Any]:
    table = _get_table_resource(session, table_name)
    return cast(
        "dict[str, Any]",
        _unfold_data(table.update_item(**kwargs, ReturnValues="ALL_NEW")["Attributes"]),
    )


deserializer = TypeDeserializer()


def ddb_low_level_object_to_normal_object(
    low_level_obj: dict[str, Any],
) -> dict[str, Any]:
    return {
        k: _unfold_data(deserializer.deserialize(v)) for k, v in low_level_obj.items()
    }


def scan_ddb(
    session: Session, table_name: str, **kwargs: Any
) -> Iterator[dict[str, Any]]:
    """
    Scan the table, with pagination handled
    """
    paginator = get_ddb_client(session).get_paginator("scan")
    for response in paginator.paginate(TableName=table_name, **kwargs):
        for item in response["Items"]:
            yield ddb_low_level_object_to_normal_object(item)


def sanitize_decimals[T](item: T) -> T:
    """
    Get rid of decimals from dynamodb outputs.
    """
    return cast("T", simplejson.loads(simplejson.dumps(item)))


def _prepare_data(item: Any) -> Any:
    if isinstance(item, dict):
        return {key: _prepare_data(value) for key, value in item.items()}
    if isinstance(item, list):
        return [_prepare_data(value) for value in item]
    if isinstance(item, float):
        return Decimal(str(item))
    return item


def _unfold_data(item: Any) -> Any:
    if isinstance(item, dict):
        return {key: _unfold_data(value) for key, value in item.items()}
    if isinstance(item, list):
        return [_unfold_data(value) for value in item]
    if isinstance(item, Decimal):
        return sanitize_decimals(item)
    if isinstance(item, Binary):
        return item.value  # type: ignore
    return item


def prepare_item(item: Any) -> dict[str, Any]:
    """
    Get rid of floats in favor of Decimal for DDB.
    """
    return cast("dict[str, Any]", _prepare_data(item))
