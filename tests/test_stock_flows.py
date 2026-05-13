from pathlib import Path
from unittest.mock import patch

from trackinsight_data_python import api, download


def test_get_stock_flows_uses_endpoint_and_format():
    with (
        patch("trackinsight_data_python.api.getPartitions") as get_partitions,
        patch(
            "trackinsight_data_python.api.getMetadata",
            return_value={"stockFlows": {"year": 2026, "month": 5}},
        ),
    ):
        api.getStockFlows(format="json")

    get_partitions.assert_called_once_with(
        endpoint="stock_flows",
        params={},
        format="json",
    )


def test_download_stock_flows_uses_endpoint_and_returns_pattern():
    data_dir = Path("trackinsight_data")

    with (
        patch("trackinsight_data_python.download.getPartitions") as get_partitions,
        patch(
            "trackinsight_data_python.download.read_vars",
            return_value=["host", "key", data_dir, 10, True],
        ),
    ):
        pattern = download.downloadStockFlows(format="json")

    get_partitions.assert_called_once_with(
        endpoint="stock_flows",
        folder="stock_flows",
        params={},
        format="json",
    )
    assert pattern == str(data_dir / "json" / "stock_flows" / "**/*.json")
