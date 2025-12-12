import typing
from functools import lru_cache

from boto3 import Session
from botocore.config import Config

from data_processing.shared.cache import lru_cache_when_not_in_test

if typing.TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_lambda.client import LambdaClient
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_sqs.client import SQSClient


@lru_cache
def get_session(region_name: str | None = None) -> Session:
    """
    Returns a new boto3 session, cached.
    Don't use it in a multi thread setting!
    """
    return Session(region_name=region_name) if region_name else Session()


DEFAULT_CONFIG = Config(connect_timeout=2)
DDB_BOTO3_CONFIG = Config(connect_timeout=1)


@lru_cache_when_not_in_test()
def get_lambda_client(session: Session) -> "LambdaClient":  # pragma: no cover
    return session.client(
        "lambda",
        config=DEFAULT_CONFIG,
    )


@lru_cache_when_not_in_test()
def get_ddb_client(session: Session) -> "DynamoDBClient":
    return session.client("dynamodb", config=DDB_BOTO3_CONFIG)


@lru_cache_when_not_in_test()
def get_s3_client(session: Session) -> "S3Client":
    return session.client("s3", config=DEFAULT_CONFIG)


@lru_cache_when_not_in_test()
def get_sqs_client(session: Session) -> "SQSClient":  # pragma: no cover
    return session.client(
        "sqs",
        config=DEFAULT_CONFIG,
    )
