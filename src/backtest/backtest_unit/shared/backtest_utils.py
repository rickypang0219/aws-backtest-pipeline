from typing import Literal
from pydantic import BaseModel


class BacktestEvent(BaseModel):
    factor_name: str
    model_name: Literal["z_score", "robust", "min_max", "percentile"]
