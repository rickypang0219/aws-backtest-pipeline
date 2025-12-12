import numpy as np
from numba import njit


@njit
def zscore_update_positions(
    position: np.ndarray,
    long_entry: np.ndarray,
    short_entry: np.ndarray,
    long_exit: np.ndarray,
    short_exit: np.ndarray,
) -> None:
    for i in range(1, len(position)):
        prev_pos = position[i - 1]
        position[i] = (
            long_entry[i] * (prev_pos == 0)
            + short_entry[i] * (prev_pos == 0)
            + prev_pos
            * (1 - long_exit[i] * (prev_pos == 1) - short_exit[i] * (prev_pos == -1))
        )


@njit
def min_max_update_positions(
    positions: np.ndarray,
    long_entry: np.ndarray,
    short_entry: np.ndarray,
    long_exit: np.ndarray,
    short_exit: np.ndarray,
) -> None:
    for i in range(1, len(positions)):
        if long_entry[i]:
            positions[i] = 1
        elif short_entry[i]:
            positions[i] = -1
        else:
            positions[i] = positions[i - 1]
        if (positions[i] == 1 and long_exit[i]) or (
            positions[i] == -1 and short_exit[i]
        ):
            positions[i] = 0


@njit
def percentile_update_positions(  # noqa: PLR0913 PLR0912
    positions: np.ndarray,
    v_values: np.ndarray,
    upper_percentile: np.ndarray,
    lower_percentile: np.ndarray,
    median_percentile: np.ndarray,
    period: int,
) -> None:
    n = len(v_values)
    for i in range(period, n):
        if positions[i - 1] == 0:
            if v_values[i] > upper_percentile[i]:
                positions[i] = 1
            elif v_values[i] < lower_percentile[i]:
                positions[i] = -1
        elif positions[i - 1] == 1:
            if v_values[i] <= median_percentile[i]:
                positions[i] = 0
            elif v_values[i] < lower_percentile[i]:
                positions[i] = -1
            else:
                positions[i] = 1
        elif positions[i - 1] == -1:
            if v_values[i] >= median_percentile[i]:
                positions[i] = 0
            elif v_values[i] > upper_percentile[i]:
                positions[i] = 1
            else:
                positions[i] = -1
        else:
            positions[i] = positions[i - 1]


@njit
def ma_diff_update_positions(
    positions: np.ndarray,
    long_entry: np.ndarray,
    short_entry: np.ndarray,
    long_exit: np.ndarray,
    short_exit: np.ndarray,
) -> None:
    for i in range(1, len(positions)):
        if long_entry[i]:
            positions[i] = 1
        elif short_entry[i]:
            positions[i] = -1
        else:
            positions[i] = positions[i - 1]
        if (positions[i] == 1 and long_exit[i]) or (
            positions[i] == -1 and short_exit[i]
        ):
            positions[i] = 0


@njit
def robust_update_positions(
    positions: np.ndarray,
    long_entry: np.ndarray,
    short_entry: np.ndarray,
    long_exit: np.ndarray,
    short_exit: np.ndarray,
) -> None:
    for i in range(1, len(positions)):
        if long_entry[i]:
            positions[i] = 1
        elif short_entry[i]:
            positions[i] = -1
        else:
            positions[i] = positions[i - 1]
        if (positions[i] == 1 and long_exit[i]) or (
            positions[i] == -1 and short_exit[i]
        ):
            positions[i] = 0


@njit
def roc_update_positions(
    positions: np.ndarray,
    long_entry: np.ndarray,
    short_entry: np.ndarray,
    long_exit: np.ndarray,
    short_exit: np.ndarray,
) -> None:
    for i in range(1, len(positions)):
        if long_entry[i]:
            positions[i] = 1
        elif short_entry[i]:
            positions[i] = -1
        else:
            positions[i] = positions[i - 1]

        if (positions[i] == 1 and long_exit[i]) or (
            positions[i] == -1 and short_exit[i]
        ):
            positions[i] = 0
