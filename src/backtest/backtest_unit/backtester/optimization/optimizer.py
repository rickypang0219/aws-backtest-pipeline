import numpy as np
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.figure as fig
import seaborn as sns

from numba import njit
from multiprocessing import Pool

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backtest_unit.backtester.framework.framework import BacktestFramework
    from backtest_unit.backtester.models.models import TradingModel


class ParameterOptimizer:
    def __init__(
        self,
        framework: "BacktestFramework",
        model: "TradingModel",
        strategy_name: str,
    ) -> None:
        self.framework = framework
        self.model = model
        self.strategy_name = strategy_name

    def compute_sharpe_with_params(self, params: tuple[int, float]) -> tuple:
        rolling_window, multiplier = params
        trade_info = self.model.get_strategy(self.strategy_name)(
            rolling_window, multiplier
        )
        trade_info = self.framework.compute_pnl(trade_info)
        trade_info = self.framework.compute_cum_pnl(trade_info)
        sharpe = self.framework.compute_sharpe_ratio(trade_info, 365)
        return (params, sharpe)

    def optimize_params_and_plot_heatmap(
        self,
        rolling_windows: np.ndarray,
        multipliers: np.ndarray,
        factor_name: str = "factor_name",
    ) -> fig.Figure:
        xy_pairs = [(xi, yi) for yi in multipliers for xi in rolling_windows]
        with Pool(processes=8) as pool:
            results = pool.map(self.compute_sharpe_with_params, xy_pairs)

        xy_pairs, z_values = zip(*results, strict=False)
        z = np.array(z_values).reshape(len(multipliers), len(rolling_windows))
        z_transposed = z.T  # This is the crucial change

        rolling_windows_expanded = np.append(
            rolling_windows,
            rolling_windows[-1] + (rolling_windows[-1] - rolling_windows[-2]),
        )
        multipliers_expanded = np.append(
            multipliers, multipliers[-1] + (multipliers[-1] - multipliers[-2])
        )
        yticklabels = [f"{x:.0f}" for x in rolling_windows_expanded]
        xticklabels = [f"{y:.3f}" for y in multipliers_expanded]  # Now x-axis labels

        fig = plt.figure(figsize=(20, 20))
        ax = sns.heatmap(
            z_transposed,
            annot=True,
            fmt=".2f",
            cmap="coolwarm_r",
            xticklabels=xticklabels,  # Swapped
            yticklabels=yticklabels,  # Swapped
            vmin=-2,
            vmax=3.5,
            center=1,
            annot_kws={"size": 20},
        )
        ax.set_xlabel("Multipliers")
        ax.set_ylabel("Rolling Windows")
        ax.set_title(f"{factor_name} Heatmap")
        plt.tight_layout()
        return fig

    def plot_returns(self, trade_info: pl.DataFrame) -> None:
        trade_info = self.framework._convert_humanized_timestamp(trade_info)
        trade_info_pd = trade_info.to_pandas()
        plt.title("Cumulative PnL of Market VS Strategy")
        plt.plot(
            trade_info_pd["humanized_timestamp"],
            trade_info_pd["benchmark_cumPnL"],
            label="Market",
        )
        plt.plot(
            trade_info_pd["humanized_timestamp"],
            trade_info_pd["strategy_cumPnL"],
            label="Strategy",
        )
        plt.legend()
        plt.xlabel("Timestamp")
        plt.ylabel("Cumulative PnL")
        plt.show()


class NumbaParameterOptimizer:
    def __init__(
        self,
        framework: "BacktestFramework",
        model: "TradingModel",
        strategy_name: str,
    ) -> None:
        self.framework = framework
        self.model = model
        self.strategy_name = strategy_name

    def compute_sharpe_with_params(self, params: tuple[int, float]) -> tuple:
        rolling_window, multiplier = params
        trade_info = self.model.get_strategy(self.strategy_name)(
            rolling_window, multiplier
        )
        trade_info = self.framework.compute_pnl(trade_info)
        trade_info = self.framework.compute_cum_pnl(trade_info)
        sharpe = self.framework.compute_sharpe_ratio(trade_info, 365)
        return (params, sharpe)

    @staticmethod
    @njit
    def _generate_param_pairs(
        rolling_windows: np.ndarray, multipliers: np.ndarray, xy_pairs: np.ndarray
    ) -> np.ndarray:
        idx = 0
        for yi in multipliers:
            for xi in rolling_windows:
                xy_pairs[idx, 0] = xi
                xy_pairs[idx, 1] = yi
                idx += 1
        return xy_pairs

    def optimize_params_and_plot_heatmap(
        self,
        rolling_windows: np.ndarray,
        multipliers: np.ndarray,
        factor_name: str = "factor_name",
    ) -> fig.Figure:
        xy_pairs = np.zeros((len(multipliers) * len(rolling_windows), 2))

        xy_pairs = self._generate_param_pairs(rolling_windows, multipliers, xy_pairs)

        results = np.zeros(len(xy_pairs))
        result_pairs = []
        for i, params in enumerate(xy_pairs):
            result = self.compute_sharpe_with_params((int(params[0]), params[1]))
            results[i] = result[1]
            result_pairs.append((tuple(params), result[1]))

        xy_pairs, z_values = zip(*result_pairs, strict=False)
        z = np.array(z_values).reshape(len(multipliers), len(rolling_windows))
        z_transposed = z.T

        rolling_windows_expanded = np.append(
            rolling_windows,
            rolling_windows[-1] + (rolling_windows[-1] - rolling_windows[-2]),
        )
        multipliers_expanded = np.append(
            multipliers, multipliers[-1] + (multipliers[-1] - multipliers[-2])
        )
        yticklabels = [f"{x:.0f}" for x in rolling_windows_expanded]
        xticklabels = [f"{y:.3f}" for y in multipliers_expanded]

        fig = plt.figure(figsize=(20, 20))
        ax = sns.heatmap(
            z_transposed,
            annot=True,
            fmt=".2f",
            cmap="coolwarm_r",
            xticklabels=xticklabels,
            yticklabels=yticklabels,
            vmin=-2,
            vmax=3.5,
            center=1,
            annot_kws={"size": 20},
        )
        ax.set_xlabel("Multipliers")
        ax.set_ylabel("Rolling Windows")
        ax.set_title(f"{factor_name} Heatmap")
        plt.tight_layout()
        return fig

    def plot_returns(self, trade_info: pl.DataFrame) -> None:
        trade_info = self.framework._convert_humanized_timestamp(trade_info)
        trade_info_pd = trade_info.to_pandas()
        plt.title("Cumulative PnL of Market VS Strategy")
        plt.plot(
            trade_info_pd["humanized_timestamp"],
            trade_info_pd["benchmark_cumPnL"],
            label="Market",
        )
        plt.plot(
            trade_info_pd["humanized_timestamp"],
            trade_info_pd["strategy_cumPnL"],
            label="Strategy",
        )
        plt.legend()
        plt.xlabel("Timestamp")
        plt.ylabel("Cumulative PnL")
        plt.show()
