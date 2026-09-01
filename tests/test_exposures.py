from pathlib import Path
from unittest.mock import patch

import polars as pl

from trackinsight_data_python import api, download, downloadExposures, getExposures


def test_get_exposures_uses_endpoint_and_ids():
    with patch("trackinsight_data_python.api.getPartitions") as get_partitions:
        getExposures(ids=[4942, 39])

    get_partitions.assert_called_once_with(
        endpoint="exposures",
        params={"ids": "4942,39"},
    )


def test_get_exposures_filters_large_id_lists_locally():
    ids = list(range(1001))
    source = pl.DataFrame({"share_id": [0, 1000, 1001], "value": [1, 2, 3]})

    with patch("trackinsight_data_python.api.getPartitions", return_value=source) as get_partitions:
        result = api.getExposures(ids=ids)

    get_partitions.assert_called_once_with(endpoint="exposures", params={})
    assert result["share_id"].to_list() == [0, 1000]


def test_download_exposures_uses_endpoint_ids_and_returns_pattern():
    data_dir = Path("trackinsight_data")

    with (
        patch("trackinsight_data_python.download.getPartitions") as get_partitions,
        patch(
            "trackinsight_data_python.download.read_vars",
            return_value=["host", "key", data_dir, 10, True],
        ),
    ):
        pattern = downloadExposures(ids=[4942, 39], format="json")

    get_partitions.assert_called_once_with(
        endpoint="exposures",
        folder="exposures",
        params={"ids": "4942,39"},
        format="json",
    )
    assert pattern == str(data_dir / "json" / "exposures" / "**/*.json")
    assert download.downloadExposures is downloadExposures
