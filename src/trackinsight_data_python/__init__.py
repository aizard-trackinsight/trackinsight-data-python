"""Public package API for trackinsight-data-python."""

from importlib import import_module

_API_EXPORTS = [
    "getMetadata",
    "getShares",
    "getTimeseries",
    "getReports",
    "getHoldings",
    "getLiquidity",
    "downloadShares",
    "downloadReports",
    "downloadTimeseries",
    "downloadHoldings",
    "downloadLiquidity",
]

__all__ = ["main", *_API_EXPORTS]


def __getattr__(name: str):
    if name in _API_EXPORTS:
        return getattr(import_module(".api", __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def main() -> None:
    print("Hello from trackinsight-data-python!")

