from trackinsight_data_python import getMonthlyTimeseries
from trackinsight_data_python import api


def test_get_monthly_timeseries_uses_monthly_endpoint(monkeypatch):
    calls = []

    def fake_get_partitions(endpoint, params):
        calls.append((endpoint, params))
        return None

    monkeypatch.setattr(api, "getPartitions", fake_get_partitions)

    result = getMonthlyTimeseries(
        start="2024-01-01",
        end="2024-12-31",
        ccy="usd",
        ids=[4942, 39],
    )

    assert result is None
    assert calls == [
        (
            "monthly_timeseries",
            {
                "from": "2024-01-01",
                "to": "2024-12-31",
                "ccy": "usd",
                "ids": "4942,39",
            },
        )
    ]
