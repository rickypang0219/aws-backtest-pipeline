import os
import s3fs
import logging
import polars as pl
import numpy as np
from typing import Literal, Any
from pydantic import BaseModel, ValidationError

from backtest_unit.backtester.models.models import TradingModel
from backtest_unit.backtester.framework.framework import BacktestFramework
from backtest_unit.backtester.optimization.optimizer import (
    NumbaParameterOptimizer,
    ParameterOptimizer,
)
from backtest_unit.shared.boto3 import get_s3_client, get_session
from backtest_unit.backtester.shared.heatmap_upload import save_plot_to_s3


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fs = s3fs.S3FileSystem(anon=False)

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME", f"data-bucket-v2-{AWS_ACCOUNT_ID}")
S3_CLIENT = get_s3_client(get_session())

THRESHOLD_DICT = {
    "percentile": np.arange(0.0, 0.5, 0.05),
    "min_max": np.arange(0.0, 0.5, 0.05),
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
    test_ratio: float = 0.2


def backtest(
    df: pl.DataFrame,
    backtest_event: IntTradesBacktestEvent,
    split_point_index: int,
    ma_window: int,
) -> None:
    model_name = backtest_event.model_name
    factor_name = backtest_event.factor_name

    _training_set = df.head(split_point_index)
    test_set = df.tail(df.height - split_point_index)

    forward_test = BacktestFramework(test_set)
    forward_model = TradingModel(test_set)
    forward_optimizer = ParameterOptimizer(forward_test, forward_model, model_name)

    rolling_windows = np.linspace(2500, 25000, 25).astype(int)

    forward_fig = forward_optimizer.optimize_params_and_plot_heatmap(
        rolling_windows,
        THRESHOLD_DICT[model_name],
        f"{model_name}#{ma_window}MA#{factor_name}#forward",
    )

    save_plot_to_s3(
        S3_CLIENT,
        forward_fig,
        BUCKET_NAME,
        f"int_trades/{backtest_event.coin_name}/heatmap/{model_name}#{ma_window}MA#{factor_name}#forward.pdf",
        "pdf",
    )


def numba_backtest(
    df: pl.DataFrame,
    backtest_event: IntTradesBacktestEvent,
    split_point_index: int,
    ma_window: int,
) -> None:
    model_name = backtest_event.model_name
    factor_name = backtest_event.factor_name

    _training_set = df.head(split_point_index)
    test_set = df.tail(df.height - split_point_index)

    forward_test = BacktestFramework(test_set)
    forward_model = TradingModel(test_set)
    forward_optimizer = NumbaParameterOptimizer(forward_test, forward_model, model_name)

    rolling_windows = np.linspace(2500, 35000, 30).astype(int)

    forward_fig = forward_optimizer.optimize_params_and_plot_heatmap(
        rolling_windows,
        THRESHOLD_DICT[model_name],
        f"{model_name}#{ma_window}MA#{factor_name}#forward",
    )

    save_plot_to_s3(
        S3_CLIENT,
        forward_fig,
        BUCKET_NAME,
        f"int_trades/{backtest_event.coin_name}/heatmap/{model_name}#{ma_window}MA#{factor_name}#forward.pdf",
        "pdf",
    )


def backtest_handler(event: dict[str, Any], _context: dict[str, Any]) -> None:
    try:
        backtest_event = IntTradesBacktestEvent(**event)
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
        logger.error("Event Error! %s", e)


if __name__ == "__main__":
    event = {
        "s3_key": "int_trades/BTCUSDT/concated_BTCUSDT.parquet",
        "coin_name": "BTCUSDT",
        "factor_name": "factor_int_price_dec_2_qty_flow",
        "model_name": "percentile",
        "test_ratio": 0.0,
    }
    backtest_handler(event, {})
