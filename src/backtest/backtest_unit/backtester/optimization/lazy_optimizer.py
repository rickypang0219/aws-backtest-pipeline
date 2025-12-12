import numpy as np
import matplotlib.pyplot as plt
import matplotlib.figure as fig
import seaborn as sns
from multiprocessing import Pool

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backtest_unit.backtester.framework.lazy_framework import BacktestFramework
    from backtest_unit.backtester.models.lazy_models import TradingModel


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
