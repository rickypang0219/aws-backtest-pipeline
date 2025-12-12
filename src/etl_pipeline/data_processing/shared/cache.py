import sys
from collections.abc import Callable
from functools import lru_cache
from typing import TypeVar


RT = TypeVar("RT")


def lru_cache_when_not_in_test(
    maxsize: int | None = None,
) -> Callable[[Callable[..., RT]], Callable[..., RT]]:
    if "unittest" in sys.modules:
        return lambda function: function
    return lru_cache(maxsize)
