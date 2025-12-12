from collections.abc import Callable
import polars as pl
import numpy as np
from backtest_unit.backtester.models.accelerate import (
    zscore_update_positions,
    percentile_update_positions,
    roc_update_positions,
    ma_diff_update_positions,
    min_max_update_positions,
    robust_update_positions,
)


class TradingModel:
    def __init__(self, df: pl.DataFrame) -> None:
        self.df = df
        self.factor = df["factor"]
        self.timestamp = df["timestamp"]
        self.strategies = {
            "z_score": self.z_score,
            "min_max": self.min_max,
            "percentile": self.percentile,
            "ma_diff": self.ma_diff,
            "robust": self.robust,
            "roc": self.roc,
        }

    def z_score(self, window: int, threshold: float) -> pl.DataFrame:
        trade_info = pl.DataFrame()
        factor_np = self.factor.to_numpy()
        rolling_mean = (
            self.factor.rolling_mean(window_size=window).fill_null(0).to_numpy()
        )
        rolling_std = (
            self.factor.rolling_std(window_size=window).fill_null(0).to_numpy()
        )
        z_score = np.divide(
            self.factor.to_numpy() - rolling_mean,
            rolling_std,
            np.zeros_like(factor_np),
            where=rolling_std != 0,
        )

        long_entry = (z_score > threshold).astype(int)
        long_exit = (z_score <= 0).astype(int)
        short_entry = (z_score < -1 * threshold).astype(int) * -1
        short_exit = (z_score >= 0).astype(int)

        trade_info = trade_info.with_columns(self.timestamp.alias("timestamp"))
        position: np.ndarray = np.zeros(len(self.timestamp))
        zscore_update_positions(
            position, long_entry, short_entry, long_exit, short_exit
        )
        return trade_info.with_columns([pl.Series("position", position)])

    def min_max(
        self,
        window: int,
        threshold: float,
    ) -> pl.DataFrame:
        trade_info = pl.DataFrame()
        factor_np = self.factor.to_numpy()
        rolling_min = self.factor.rolling_min(window_size=window).to_numpy()
        rolling_max = self.factor.rolling_max(window_size=window).to_numpy()
        valid_mask = (rolling_max - rolling_min) != 0
        scaled_values = np.empty_like(factor_np)
        scaled_values[valid_mask] = (
            factor_np[valid_mask] - rolling_min[valid_mask]
        ) / (rolling_max[valid_mask] - rolling_min[valid_mask])
        long_entry = scaled_values > (1 - threshold)
        long_exit = scaled_values <= 0.5
        short_entry = scaled_values < threshold
        short_exit = scaled_values >= 0.5
        trade_info = trade_info.with_columns(self.timestamp.alias("timestamp"))
        position: np.ndarray = np.zeros(len(self.timestamp))
        min_max_update_positions(
            position, long_entry, short_entry, long_exit, short_exit
        )
        return trade_info.with_columns([pl.Series("position", position)])

    def percentile(self, period: int, threshold: float) -> pl.DataFrame:
        trade_info = pl.DataFrame()
        upper_percentile = (
            self.factor.rolling_quantile(
                quantile=1 - threshold - 1e-8,
                window_size=period,
                interpolation="linear",
            )
            .fill_null(0)
            .to_numpy()
        )
        lower_percentile = (
            self.factor.rolling_quantile(
                quantile=threshold + 1e-8, window_size=period, interpolation="linear"
            )
            .fill_null(0)
            .to_numpy()
        )
        median_percentile = (
            self.factor.rolling_quantile(
                quantile=0.5, window_size=period, interpolation="linear"
            )
            .fill_null(0)
            .to_numpy()
        )

        trade_info = trade_info.with_columns(self.timestamp.alias("timestamp"))
        position: np.ndarray = np.zeros(len(self.timestamp))
        percentile_update_positions(
            position,
            self.factor.to_numpy(),
            upper_percentile,
            lower_percentile,
            median_percentile,
            period,
        )
        return trade_info.with_columns([pl.Series("position", position)])

    def ma_diff(self, window: int, threshold: float) -> pl.DataFrame:
        trade_info = pl.DataFrame()
        rolling_mean = (
            self.factor.rolling_mean(window_size=window).fill_null(0).to_numpy()
        )
        diff = self.factor.to_numpy() - rolling_mean
        abs_rolling_mean = np.abs(rolling_mean)
        ma_diff = np.divide(
            diff,
            abs_rolling_mean,
            out=np.zeros_like(diff, dtype=float),
            where=abs_rolling_mean != 0,
        )
        long_entry = (ma_diff > threshold).astype(int)
        long_exit = (ma_diff <= 0).astype(int)
        short_entry = (ma_diff < -threshold).astype(int) * -1
        short_exit = (ma_diff >= 0).astype(int)
        trade_info = trade_info.with_columns(self.timestamp.alias("timestamp"))
        position: np.ndarray = np.zeros(len(self.timestamp))
        ma_diff_update_positions(
            position,
            long_entry,
            short_entry,
            long_exit,
            short_exit,
        )
        return trade_info.with_columns([pl.Series("position", position)])

    def get_strategy(self, strategy_name: str) -> Callable:
        if strategy_name not in self.strategies:
            raise ValueError(
                f"Strategy '{strategy_name}' not found."
                f"Available strategies: {list(self.strategies.keys())}"
            )
        return self.strategies[strategy_name]

    def robust(self, period: int, threshold: float) -> pl.DataFrame:
        trade_info = pl.DataFrame()

        upper_percentile = (
            self.factor.rolling_quantile(quantile=0.75, window_size=period)
            .fill_null(0)
            .to_numpy()
        )
        lower_percentile = (
            self.factor.rolling_quantile(quantile=0.25, window_size=period)
            .fill_null(0)
            .to_numpy()
        )
        median_percentile = (
            self.factor.rolling_quantile(quantile=0.5, window_size=period)
            .fill_null(0)
            .to_numpy()
        )

        iqr = upper_percentile - lower_percentile
        factor_np = self.factor.to_numpy()
        scaled_values = np.zeros_like(factor_np, dtype=float)

        np.divide(
            factor_np - median_percentile,
            iqr,
            out=scaled_values,
            where=iqr != 0,
        )

        scaled_values = np.nan_to_num(scaled_values)

        long_entry = scaled_values > threshold
        long_exit = scaled_values <= 0
        short_entry = scaled_values < -threshold
        short_exit = scaled_values >= 0

        trade_info = trade_info.with_columns(self.timestamp.alias("timestamp"))
        position: np.ndarray = np.zeros(len(self.timestamp))
        robust_update_positions(
            position, long_entry, short_entry, long_exit, short_exit
        )
        return trade_info.with_columns([pl.Series("position", position)])

    def roc(self, period: int, threshold: float) -> pl.DataFrame:
        trade_info = pl.DataFrame()
        shifted_v = self.factor.shift(n=period).fill_nan(0)
        denominator = shifted_v.abs().fill_nan(0).to_numpy()
        diff = (self.factor - shifted_v).to_numpy()
        roc = np.divide(
            diff,
            denominator,
            out=np.zeros_like(diff, dtype=float),
            where=denominator != 0,
        )
        long_entry = (roc > threshold).astype(int)
        long_exit = (roc <= 0).astype(int)
        short_entry = (roc < -threshold).astype(int) * -1
        short_exit = (roc >= 0).astype(int)
        trade_info = trade_info.with_columns(self.timestamp.alias("timestamp"))
        position: np.ndarray = np.zeros(len(self.timestamp))
        roc_update_positions(position, long_entry, short_entry, long_exit, short_exit)
        return trade_info.with_columns([pl.Series("position", position)])
