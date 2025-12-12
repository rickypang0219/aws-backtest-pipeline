import polars as pl
import numpy as np


class BacktestFramework:
    def __init__(
        self, factors_df: pl.DataFrame, transaction_cost: float = 0.06 / 100
    ) -> None:
        if not factors_df.is_empty():
            self.factors = factors_df.drop_nulls().sort("timestamp")
        else:
            self.factors = factors_df
        self.TRANSACTION_COST = transaction_cost
        print(
            f"BacktestFramework initialized with {len(self.factors)} rows of factor data."
        )

    def compute_trans_cost(self, trade_info: pl.DataFrame) -> pl.DataFrame:
        return trade_info.with_columns(
            (
                abs(pl.col("position") - pl.col("position").shift(1))
                * self.TRANSACTION_COST
            ).alias("trans_cost")
        )

    def compute_pnl(self, trade_info: pl.DataFrame) -> pl.DataFrame:
        trade_info = self.compute_trans_cost(trade_info)
        trade_info = trade_info.with_columns(
            self.factors["price"].pct_change().alias("returns")
        )
        return trade_info.with_columns(
            (
                pl.col("position").shift(1) * pl.col("returns") - pl.col("trans_cost")
            ).alias("PnL")
        )

    def compute_cum_pnl(self, trade_info: pl.DataFrame) -> pl.DataFrame:
        return trade_info.with_columns(
            pl.col("PnL").cum_sum().alias("strategy_cumPnL"),
            pl.col("returns").cum_sum().alias("benchmark_cumPnL"),
        )

    def compute_sharpe_ratio(
        self, trade_info: pl.DataFrame, trading_days: int
    ) -> float:
        trade_info = self._convert_humanized_timestamp(trade_info)
        daily_pnl = trade_info.group_by(
            pl.col("humanized_timestamp").dt.truncate("1d")
        ).agg(pl.col("PnL").sum().alias("aggPnL"))
        agg_pnl = daily_pnl["aggPnL"].drop_nulls().to_list()
        if agg_pnl:
            mean, sd = np.mean(agg_pnl), np.std(agg_pnl, ddof=1)
            return (mean / sd) * np.sqrt(trading_days) if sd != 0 else 0
        return 0

    def compute_information_ratio(
        self, trade_info: pl.DataFrame, trading_days: int
    ) -> float:
        trade_info = self._convert_humanized_timestamp(trade_info)
        daily_data = trade_info.group_by(
            pl.col("humanized_timestamp").dt.truncate("1d")
        ).agg(
            [
                pl.col("PnL").sum().alias("aggPnL"),
                pl.col("returns").sum().alias("aggReturns"),
            ]
        )
        agg_pnl, agg_returns = (
            daily_data["aggPnL"].drop_nulls().to_numpy(),
            daily_data["aggReturns"].drop_nulls().to_numpy(),
        )
        if len(agg_pnl) and len(agg_returns):
            excess = agg_pnl - agg_returns
            mean, sd = np.mean(excess), np.std(excess, ddof=1)
            return (mean / sd) * np.sqrt(trading_days) if sd != 0 else 0
        return 0

    def compute_max_drawdown(self, trade_info: pl.DataFrame) -> float:
        returns = trade_info["PnL"].drop_nulls().to_numpy()
        cum_prod_returns = np.cumprod(1 + returns)
        running_max = np.maximum.accumulate(cum_prod_returns)
        drawdown = cum_prod_returns / running_max - 1
        return np.min(drawdown)

    def compute_long_short_ratio(self, trade_info: pl.DataFrame) -> float | None:
        long_count = trade_info.filter(pl.col("position") == 1).shape[0]
        short_count = trade_info.filter(pl.col("position") == -1).shape[0]
        return long_count / short_count if short_count and long_count else None

    def _convert_humanized_timestamp(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.from_epoch("timestamp", time_unit="ms").alias("humanized_timestamp")
        )
