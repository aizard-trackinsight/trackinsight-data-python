IDS_LIMIT = 1000

DEFAULT_REPORT_PERIODS = [
    "one-day",
    "one-week",
    "month-to-date",
    "three-month-to-date",
    "year-to-date",
    "one-year-to-date",
    "three-year",
]


def _normalize_ccy(ccy):
    return "eur" if ccy is None else ccy


def _add_ids_param(params, ids):
    if ids is not None and len(ids) <= IDS_LIMIT:
        params["ids"] = ",".join(str(i) for i in ids)
    return params


def should_filter_ids_locally(ids):
    return ids is not None and len(ids) > IDS_LIMIT


def build_shares_params():
    return {}


def build_timeseries_params(start="2019-01-01", end=None, ccy="eur", ids=None):
    params = {"from": start, "to": end, "ccy": _normalize_ccy(ccy)}
    return _add_ids_param(params, ids)


def build_reports_params(
    stamp=None,
    ccy="eur",
    ids=None,
    periods=None,
    metadata_loader=None,
):
    ccy = _normalize_ccy(ccy)

    if stamp is None:
        if metadata_loader is None:
            raise ValueError("metadata_loader is required when stamp is None")
        stamp = max(metadata_loader()["reportsAsOf"][ccy])

    if periods is None:
        periods = DEFAULT_REPORT_PERIODS

    params = {
        "stamp": stamp,
        "ccy": ccy,
        "columns": "*",
        "periods": ",".join(periods),
    }
    return _add_ids_param(params, ids), stamp


def build_holdings_params(ids=None, proxy=True, level=0, extraLines=False):
    params = {
        "proxy": "true" if proxy else "false",
        "level": level,
        "extraLines": "true" if extraLines else "false",
    }
    return _add_ids_param(params, ids)


def build_liquidity_params(start, end, ccy="eur", ids=None):
    params = {"from": start, "to": end, "ccy": _normalize_ccy(ccy)}
    return _add_ids_param(params, ids)
