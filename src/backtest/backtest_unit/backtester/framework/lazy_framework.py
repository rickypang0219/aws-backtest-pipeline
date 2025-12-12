import polars as pl
import numpy as np


class BacktestFramework:
    def __init__(
        self, factors_ldf: pl.LazyFrame, transaction_cost: float = 0.06 / 100
    ) -> None:
        self.TRANSACTION_COST = transaction_cost
        self.factor_ldf = factors_ldf

    def compute_trans_cost(self, trade_info: pl.LazyFrame) -> pl.LazyFrame:
        return trade_info.with_columns(
            (
                abs(pl.col("position") - pl.col("position").shift(1))
                * self.TRANSACTION_COST
            ).alias("trans_cost")
        )

    def compute_pnl(self, trade_info: pl.LazyFrame) -> pl.LazyFrame:
        trade_info = self.compute_trans_cost(trade_info)
        trade_info = trade_info.join(
            self.factor_ldf.select(["timestamp", "price"]), on="timestamp", how="left"
        )
        trade_info = trade_info.with_columns(
            pl.col("price").pct_change().alias("returns"),
        )
        return trade_info.with_columns(
            (
                pl.col("position").shift(1) * pl.col("returns") - pl.col("trans_cost")
            ).alias("PnL"),
        )

    def compute_cum_pnl(self, trade_info: pl.LazyFrame) -> pl.LazyFrame:
        return trade_info.with_columns(
            pl.col("PnL").cum_sum().alias("strategy_cumPnL"),
            pl.col("returns").cum_sum().alias("benchmark_cumPnL"),
        )

    def compute_sharpe_ratio(
        self, trade_info: pl.LazyFrame, trading_days: int
    ) -> float:
        trade_info = trade_info.with_columns(
            pl.from_epoch("timestamp", time_unit="ms").alias("time_dt")
        )

        agg_pnl = (
            trade_info.group_by_dynamic(
                index_column="time_dt",
                every="1d",
                period="1d",
                offset="0s",
                closed="left",
            )
            .agg(pl.col("PnL").sum().alias("aggPnL"))
            .select("aggPnL")
            .fill_null(0)
            .collect()
            .to_numpy()
        )
        mean, sd = np.mean(agg_pnl), np.std(agg_pnl, ddof=1)
        return (mean / sd) * np.sqrt(trading_days) if sd != 0 else 0
