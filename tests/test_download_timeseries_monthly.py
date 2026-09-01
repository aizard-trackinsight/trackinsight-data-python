from pathlib import Path
from unittest.mock import patch

from trackinsight_data_python import download, downloadMonthlyTimeseries


def test_download_monthly_timeseries_uses_monthly_endpoint_and_returns_pattern():
    data_dir = Path("trackinsight_data")

    with (
        patch("trackinsight_data_python.download.getPartitions") as get_partitions,
        patch(
            "trackinsight_data_python.download.read_vars",
            return_value=["host", "key", data_dir, 10, True],
        ),
    ):
        pattern = downloadMonthlyTimeseries(
            start="2024-01-01",
            end="2024-12-31",
            ccy="usd",
            format="json",
        )

    get_partitions.assert_called_once_with(
        endpoint="monthly_timeseries",
        folder="usd_monthly_timeseries",
        params={
            "from": "2024-01-01",
            "to": "2024-12-31",
            "ccy": "usd",
        },
        format="json",
    )
    assert pattern == str(data_dir / "json" / "usd_monthly_timeseries" / "**/*.json")


def test_download_module_exposes_download_monthly_timeseries():
    assert download.downloadMonthlyTimeseries is downloadMonthlyTimeseries
