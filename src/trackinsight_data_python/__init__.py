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
    "contains_any",
    "contains_all",
    "single_among",
    "contains_exact",
    "contains_any_joined",
]

__all__ = ["main", *_API_EXPORTS]

_EXPORT_MODULES = {
    "getMetadata": ".api",
    "getShares": ".api",
    "getTimeseries": ".api",
    "getReports": ".api",
    "getHoldings": ".api",
    "getLiquidity": ".api",
    "downloadShares": ".download",
    "downloadReports": ".download",
    "downloadTimeseries": ".download",
    "downloadHoldings": ".download",
    "downloadLiquidity": ".download",
    "contains_any": ".helpers",
    "contains_all": ".helpers",
    "single_among": ".helpers",
    "contains_exact": ".helpers",
    "contains_any_joined": ".helpers",
}


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is not None:
        return getattr(import_module(module_name, __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def main() -> None:
    print("Hello from trackinsight-data-python!")
