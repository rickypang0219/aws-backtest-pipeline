import os
import s3fs
import logging
import polars as pl

from typing import Any
from pydantic import ValidationError
from backtest_unit.int_trades.utils import (
    IntTradesBacktestEvent,
    BUCKET_NAME,
    numba_backtest,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fs = s3fs.S3FileSystem(anon=False)

S3_KEY = os.environ.get("S3_KEY")
COIN_NAME = os.environ.get("COIN_NAME")
FACTOR_NAME = os.environ.get("FACTOR_NAME")
MODEL_NAME = os.environ.get("MODEL_NAME", "z_score")
TEST_RATIO = os.environ.get("TEST_RATIO")


def validate_required_configs(
    s3_key: str | None,
    factor_name: str | None,
    coin_name: str | None,
    model_name: str | None,
) -> dict[str, Any]:
    required_params = {
        "s3_key": s3_key,
        "factor_name": factor_name,
        "coin_name": coin_name,
        "model_name": model_name,
    }
    for param_name, param_value in required_params.items():
        if param_value is None or param_value == "":
            raise ValueError(f"Missing or invalid required parameter: {param_name}")

    return required_params


def _main() -> None:
    try:
        event = validate_required_configs(S3_KEY, FACTOR_NAME, COIN_NAME, MODEL_NAME)
        backtest_event = IntTradesBacktestEvent(
            **{
                **event,
                "test_ratio": TEST_RATIO,
            }
        )
        s3_key = backtest_event.s3_key
        factor_name = backtest_event.factor_name
        test_ratio = backtest_event.test_ratio
        s3_complete_path = f"s3://{BUCKET_NAME}/{s3_key}"
        print(s3_complete_path)

        df = (
            pl.scan_parquet(s3_complete_path)
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
            .filter(pl.col("timestamp") >= 1672531200000)  # start from 2023
        ).collect()

        split_point_index = int(df.height * test_ratio)

        for ma_window in [1000, 2000, 4000, 8000]:
            ma_df = df.with_columns(
                (
                    (pl.col(f"{factor_name}") / pl.col("total_volume"))
                    .rolling_mean(window_size=int(ma_window))
                    .fill_null(0)
                ).alias("factor"),
            )
            numba_backtest(
                ma_df,
                backtest_event,
                split_point_index,
                ma_window,
            )
    except ValidationError as e:
        logger.error("Event Deserialization Fails %s", e)
        return
    except ValueError as e:
        logger.error("Missing Argument %s", e)


if __name__ == "__main__":
    _main()
