from collections.abc import Callable
import polars as pl
import numpy as np

from backtest_unit.backtester.models.accelerate import (
    zscore_update_positions,
)


class TradingModel:
    def __init__(self, factor_ldf: pl.LazyFrame) -> None:
        self.factor_ldf = factor_ldf
        self.strategies = {
            "z_score": self.z_score,
        }

    def z_score(
        self,
        window: int,
        threshold: float,
    ) -> pl.LazyFrame:
        """
        Corrected lazy implementation of the z-score strategy.
        Builds the lazy plan sequentially before collecting.
        """
        ldf_with_rolls = self.factor_ldf.with_columns(
            [
                pl.col("factor")
                .rolling_mean(window_size=window)
                .fill_null(0)
                .alias("rolling_mean"),
                pl.col("factor")
                .rolling_std(window_size=window)
                .fill_null(0)
                .alias("rolling_std"),
            ]
        )

        ldf_with_zscore = ldf_with_rolls.with_columns(
            pl.when(pl.col("rolling_std") != 0)
            .then((pl.col("factor") - pl.col("rolling_mean")) / pl.col("rolling_std"))
            .otherwise(0)
            .fill_null(0)
            .alias("z_score"),
        ).select(["timestamp", "z_score"])

        z_score_np = ldf_with_zscore.select("z_score").collect().to_numpy().flatten()

        long_entry = (z_score_np > threshold).astype(int)
        long_exit = (z_score_np <= 0).astype(int)
        short_entry = (z_score_np < -1 * threshold).astype(int) * -1
        short_exit = (z_score_np >= 0).astype(int)
        position: np.ndarray = np.zeros(len(z_score_np))

        zscore_update_positions(
            position, long_entry, short_entry, long_exit, short_exit
        )

        # Step 5: Create a new LazyFrame from the eager position data
        # and re-combine with the timestamp column from the original lazy plan.
        # We collect the timestamp column to ensure the returned data is complete.
        timestamp_np = (
            ldf_with_zscore.select("timestamp").collect().to_numpy().flatten()
        )
        eager_df = pl.DataFrame({"timestamp": timestamp_np, "position": position})
        return eager_df.lazy()

    def get_strategy(self, strategy_name: str) -> Callable:
        if strategy_name not in self.strategies:
            raise ValueError(
                f"Strategy '{strategy_name}' not found."
                f"Available strategies: {list(self.strategies.keys())}"
            )
        return self.strategies[strategy_name]
