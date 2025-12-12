import json
import numpy as np
import itertools
from typing import Any, Literal
from pydantic import BaseModel, ValidationError


THRESHOLD_DICT = {
    "percentile": np.arange(0.0, 0.55, 0.05),
    "min_max": np.arange(0.0, 0.55, 0.05),
    "z_score": np.arange(0.0, 3.2, 0.2),
    "robust": np.arange(0.0, 3.2, 0.2),
    "ma_diff": np.arange(0.0, 3.2, 0.2),
    "roc": np.arange(0.0, 3.2, 0.2),
}

FILTER_NAMES = [
    "int_price_qty",
    "int_price_int_qty",
    "int_price_dec_1_qty",
    "int_price_dec_2_qty",
    "int10_price_int_qty",
    "int10_price_dec_1_qty",
    "int10_price_dec_2_qty",
    "int_price_quote_qty",
    "int_price_int_quote_qty",
    "int_price_int10_quote_qty",
    "int_price_int100_quote_qty",
    "int_price_int1000_quote_qty",
    "int10_price_int_quote_qty",
    "int10_price_int10_quote_qty",
    "int10_price_int100_quote_qty",
    "int10_price_int1000_quote_qty",
]


class FactorAssignEvent(BaseModel):
    coin_name: str
    model_name: Literal["z_score", "roc", "min_max", "percentile", "ma_diff", "robust"]
    s3_key: str  # location of the concated parquet file
    test_ratio: float | None = 0.2


class ParamsAssignEvent(BaseModel):
    coin_name: str
    model_name: str
    s3_key: str
    train_ratio: float
    factor_name: str


def _generate_combinatorial_filters(
    base_conditions: list[str],
) -> list[str]:
    condition_names = base_conditions

    and_filters = [
        "_AND_".join(combo_names)
        for r in range(2, len(condition_names) + 1)
        for combo_names in itertools.combinations(condition_names, r)
    ]

    or_filters = [
        "_OR_".join(combo_names)
        for r in range(2, len(condition_names) + 1)
        for combo_names in itertools.combinations(condition_names, r)
    ]

    return base_conditions + and_filters + or_filters


def assign_tasks_handler(
    event: dict[str, Any], _context: dict[str, Any]
) -> dict[str, Any]:
    try:
        assign_factor_event = FactorAssignEvent(**event)
        coin_name = assign_factor_event.coin_name
        model_name = assign_factor_event.model_name
        s3_key = assign_factor_event.s3_key
        test_ratio = assign_factor_event.test_ratio
        all_flow_factor_names = [f"factor_{i}_flow" for i in FILTER_NAMES]
        return {
            "statusCode": 200,
            "coin_name": coin_name,
            "model_name": model_name,
            "s3_key": s3_key,
            "test_ratio": test_ratio,
            "tasks": all_flow_factor_names,
        }
    except ValidationError as e:
        print("Validation Error %s", e)
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Invalid Event {e}"}),
        }


def assign_params_handler(
    event: dict[str, Any], _context: dict[str, Any]
) -> dict[str, Any]:
    try:
        params_event = ParamsAssignEvent(**event)
        model_name = params_event.model_name
        rolling_windows = np.linspace(2500, 25000, 25).astype(int)
        thresholds = THRESHOLD_DICT[model_name]
        xy_pairs = [
            (int(xi), round(float(yi), 3))
            for yi in thresholds
            for xi in rolling_windows
        ]
        return {**event, "params": xy_pairs}
    except ValidationError as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Invalid Event {e}"}),
        }


if __name__ == "__main__":
    from backtest_unit.int_trades.bt_handler import backtest_handler

    event = {
        "coin_name": "BTCUSDT",
        "model_name": "z_score",
        "s3_key": "int_trades/BTCUSDT/concated_BTCUSDT.parquet",
        "test_ratio": 0.2,
    }

    assign_result = assign_tasks_handler(
        event,
        {},
    )

    if assign_result:
        for _task in [assign_result["tasks"][0]]:
            params_result = assign_params_handler(
                {
                    **event,
                    "factor_name": "factor_price_is_int_AND_qty_2_decimals_flow",
                },
                {},
            )
            if params_result:
                print(params_result)
                for params in params_result["params"]:
                    print(backtest_handler({**params_result, "params": params}, {}))
