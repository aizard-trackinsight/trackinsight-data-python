import os

import pytest

from trackinsight_data_python import api


def _require_api_runtime() -> None:
    try:
        __import__("polars")
    except ModuleNotFoundError as exc:
        pytest.fail(f"Missing required dependency for integration tests: {exc.name}")

    try:
        __import__("dotenv")
    except ModuleNotFoundError:
        pytest.fail("Missing required dependency for integration tests: python-dotenv")
    from dotenv import load_dotenv

    load_dotenv()

    missing = [
        key
        for key in ("API_HOST", "API_KEY", "API_STORAGE")
        if not os.getenv(key)
    ]
    if missing:
        pytest.fail(f"Missing required env vars for API integration tests: {', '.join(missing)}")


def test_get_metadata_does_not_raise():
    _require_api_runtime()
    api.getMetadata()


def test_get_shares_does_not_raise():
    _require_api_runtime()
    api.getShares()


def test_get_reports_does_not_raise():
    _require_api_runtime()
    api.getReports(ids=[4942, 39])


def test_get_holdings_does_not_raise():
    _require_api_runtime()
    api.getHoldings(ids=[4942, 39])


def test_get_timeseries_does_not_raise():
    _require_api_runtime()
    api.getTimeseries(ids=[4942, 39])


def test_get_liquidity_does_not_raise():
    _require_api_runtime()
    api.getLiquidity(start="2026-01-01", end="2026-01-31")
