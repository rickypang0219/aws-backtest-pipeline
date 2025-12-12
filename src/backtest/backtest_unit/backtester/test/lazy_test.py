import logging
import polars as pl
import numpy as np
from typing import Literal, Any
from pydantic import BaseModel, ValidationError


from backtest_unit.backtester.models.lazy_models import TradingModel
from backtest_unit.backtester.framework.lazy_framework import BacktestFramework
from backtest_unit.backtester.optimization.lazy_optimizer import ParameterOptimizer


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
    train_ratio: float = 0.8


def backtest(
    ldf: pl.LazyFrame,
    backtest_event: IntTradesBacktestEvent,
    split_point_index: int,
    ma_window: int,
) -> None:
    model_name = backtest_event.model_name
    factor_name = backtest_event.factor_name

    training_set = ldf.limit(split_point_index)
    backtest = BacktestFramework(training_set)
    model = TradingModel(training_set)
    optimizer = ParameterOptimizer(backtest, model, model_name)

    rolling_windows = np.linspace(2500, 25000, 25).astype(int)

    backtest_fig = optimizer.optimize_params_and_plot_heatmap(
        rolling_windows,
        THRESHOLD_DICT[model_name],
        f"{model_name}#{ma_window}MA#{factor_name}#training",
    )

    backtest_fig.savefig("test.pdf")


def backtest_handler(event: dict[str, Any], _context: dict[str, Any]) -> None:
    try:
        backtest_event = IntTradesBacktestEvent(**event)
        factor_name = backtest_event.factor_name
        train_ratio = backtest_event.train_ratio

        ldf = (
            pl.scan_parquet("concated_BTCUSDT.parquet")
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
        )

        split_point_index = int(ldf.select(pl.len()).collect().item() * train_ratio)

        for ma_window in [
            1000,
            # 2000,
            # 4000,
            # 8000,
        ]:
            ma_df = ldf.with_columns(
                (
                    (pl.col(f"{factor_name}") / pl.col("total_volume"))
                    .rolling_mean(window_size=int(ma_window))
                    .fill_null(0)
                ).alias("factor"),
            )
            backtest(
                ma_df,
                backtest_event,
                split_point_index,
                ma_window,
            )
    except ValidationError as e:
        logger.error("Event Error! %s", e)


if __name__ == "__main__":
    event = {
        "s3_key": "BTCUSDT/concated_BTCUSDT.parquet",
        "coin_name": "BTCUSDT",
        "factor_name": "factor_price_is_int_AND_qty_2_decimals_flow",
        "model_name": "z_score",
        "train_ratio": 0.8,
    }
    backtest_handler(event, {})
