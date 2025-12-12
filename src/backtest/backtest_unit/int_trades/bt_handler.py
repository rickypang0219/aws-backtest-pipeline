import os
import json
import s3fs
import logging
import polars as pl
import numpy as np


from typing import Literal, Any
from botocore.exceptions import ClientError
from pydantic import BaseModel, ValidationError


from backtest_unit.backtester.models.models import TradingModel
from backtest_unit.backtester.framework.framework import BacktestFramework
from backtest_unit.backtester.optimization.optimizer import ParameterOptimizer
from backtest_unit.shared.boto3 import get_s3_client, get_session


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fs = s3fs.S3FileSystem(anon=False)

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME", f"data-bucket-{AWS_ACCOUNT_ID}")
S3_CLIENT = get_s3_client(get_session())

THRESHOLD_DICT = {
    "percentile": np.arange(0.0, 0.55, 0.05),
    "min_max": np.arange(0.0, 0.55, 0.05),
    "z_score": np.arange(0.0, 3.2, 0.2),
    "robust": np.arange(0.0, 3.2, 0.2),
    "ma_diff": np.arange(0.0, 3.2, 0.2),
    "roc": np.arange(0.0, 3.2, 0.2),
}


class IntTradesBacktestEvent(BaseModel):
    s3_key: str
    coin_name: str
    factor_name: str
    model_name: Literal[
        "z_score",
        "min_max",
        "percentile",
        "ma_diff",
        "robust",
        "roc",
    ]
    params: tuple[int, float]
    train_ratio: float = 0.8


def _call_compute_sharpe(
    df: pl.DataFrame,
    backtest_event: IntTradesBacktestEvent,
    split_point_index: int,
) -> tuple:
    training_set = df.head(split_point_index)

    backtest = BacktestFramework(training_set)
    model = TradingModel(training_set)
    optimizer = ParameterOptimizer(backtest, model, backtest_event.model_name)

    return optimizer.compute_sharpe_with_params(backtest_event.params)


def backtest_handler(event: dict[str, Any], _context: dict[str, Any]) -> dict[str, Any]:
    try:
        bt_event = IntTradesBacktestEvent(**event)
        s3_key = bt_event.s3_key
        factor_name = bt_event.factor_name
        train_ratio = bt_event.train_ratio

        df = (
            pl.scan_parquet(f"s3://{BUCKET_NAME}/{s3_key}")
            .select(["time_dt", "total_volume", "close", f"{factor_name}"])
            .with_columns(
                [
                    (
                        pl.col("time_dt")
                        .dt.timestamp(time_unit="ms")
                        .cast(pl.Int64)
                        .alias("timestamp")
                    )
                ]
            )
            .rename({"close": "price"})
            .select(["timestamp", "price", f"{factor_name}", "total_volume"])
            .sort("timestamp")
        ).collect()

        split_point_index = int(df.height * train_ratio)

        sharpe_dict = {}

        for ma_window in [
            1000,
            2000,
            4000,
            8000,
        ]:
            ma_df = df.with_columns(
                (
                    (pl.col(f"{factor_name}") / pl.col("total_volume"))
                    .rolling_mean(window_size=int(ma_window))
                    .fill_null(0)
                ).alias("factor"),
            )
            sharpe_dict[f"{ma_window}MA"] = _call_compute_sharpe(
                ma_df,
                bt_event,
                split_point_index,
            )

        return {"sharpe_dict": sharpe_dict}

    except ValidationError:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid Event Type!"}),
        }
    except ClientError as e:
        print(e)
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "No such S3 Key "}),
        }


if __name__ == "__main__":
    event = {
        "s3_key": "BTCUSDT/concated_BTCUSDT.parquet",
        "coin_name": "BTCUSDT",
        "factor_name": "int_price_dec_2_qty",
        "model_name": "z_score",
        "train_ratio": 0.0,
        "params": (11875, 0.6),
    }
    print(backtest_handler(event, {}))
