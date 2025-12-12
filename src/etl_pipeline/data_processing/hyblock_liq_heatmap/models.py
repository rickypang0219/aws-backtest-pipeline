from typing import Literal
from pydantic import BaseModel


class InitEvent(BaseModel):
    symbol: str
    exchange: Literal[
        "binance_perp_stable",
        "bitmex_perp_coin",
        "bybit_perp_coin",
    ]
    lookback: Literal[
        "12h",
        "1d",
        "3d",
        "7d",
        "1m",
        "3m",
        "6m",
        "1y",
        "2y",
    ]
    scaling: Literal[
        "max",
        "relative",
    ]
    leverage: Literal[
        "L1",
        "L2",
        "L3",
        "L4",
        "L5",
        "all",
    ]


class FetchEvent(BaseModel):
    symbol: str
    exchange: Literal[
        "binance_perp_stable",
        "bitmex_perp_coin",
        "bybit_perp_coin",
    ]
    lookback: Literal[
        "12h",
        "1d",
        "3d",
        "7d",
        "1m",
        "3m",
        "6m",
        "1y",
        "2y",
    ]
    scaling: Literal[
        "max",
        "relative",
    ]
    leverage: Literal[
        "L1",
        "L2",
        "L3",
        "L4",
        "L5",
        "all",
    ]
    start_ts: int


class SqsMessage(BaseModel):
    zip_data: str
    timestamp: int
    s3_postfix: str
