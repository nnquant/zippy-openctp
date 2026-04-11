import pytest
import zippy_openctp
from zippy_openctp import utils


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        raise AssertionError("json.load should read directly from the response object")


def test_package_exports_utils_queries():
    assert callable(zippy_openctp.get_markets)
    assert callable(zippy_openctp.get_products)
    assert callable(zippy_openctp.get_instruments)
    assert callable(zippy_openctp.get_quotes)
    assert callable(zippy_openctp.get_sessions)


def test_get_markets_uses_expected_endpoint(monkeypatch):
    captured = {}

    def fake_fetch(url: str, timeout_sec: float):
        captured["url"] = url
        captured["timeout_sec"] = timeout_sec
        return {"rsp_code": 0, "rsp_message": "succeed", "data": [{"ExchangeID": "SHFE"}]}

    monkeypatch.setattr(utils, "_fetch_json", fake_fetch)

    rows = utils.get_markets(timeout_sec=3.5)

    assert rows == [{"ExchangeID": "SHFE"}]
    assert captured["url"] == "http://dict.openctp.cn/markets"
    assert captured["timeout_sec"] == 3.5


def test_get_products_normalizes_common_filters(monkeypatch):
    captured = {}

    def fake_fetch(url: str, timeout_sec: float):
        captured["url"] = url
        return {"rsp_code": 0, "rsp_message": "succeed", "data": []}

    monkeypatch.setattr(utils, "_fetch_json", fake_fetch)

    utils.get_products(
        area="China",
        markets=["SHFE", "DCE"],
        type="options",
        products="au_o,rb_o",
    )

    assert (
        captured["url"]
        == "http://dict.openctp.cn/products?area=China&markets=SHFE%2CDCE&type=options&products=au_o%2Crb_o"
    )


def test_get_instruments_supports_inst_life_phase(monkeypatch):
    captured = {}

    def fake_fetch(url: str, timeout_sec: float):
        captured["url"] = url
        return {"rsp_code": 0, "rsp_message": "succeed", "data": []}

    monkeypatch.setattr(utils, "_fetch_json", fake_fetch)

    utils.get_instruments(markets="SHFE", inst_life_phase=3)

    assert captured["url"] == "http://dict.openctp.cn/instruments?markets=SHFE&InstLifePhase=3"


def test_get_quotes_and_sessions_reuse_common_param_builder(monkeypatch):
    seen = []

    def fake_fetch(url: str, timeout_sec: float):
        seen.append(url)
        return {"rsp_code": 0, "rsp_message": "succeed", "data": []}

    monkeypatch.setattr(utils, "_fetch_json", fake_fetch)

    utils.get_quotes(markets=["CFFEX"], products=["HO"])
    utils.get_sessions(markets=["CFFEX"], products=["HO"])

    assert seen == [
        "http://dict.openctp.cn/quotes?markets=CFFEX&products=HO",
        "http://dict.openctp.cn/sessions?markets=CFFEX&products=HO",
    ]


def test_multi_value_filters_reject_empty_lists():
    with pytest.raises(ValueError, match="markets must not resolve to an empty value list"):
        utils.get_products(markets=[])


def test_non_zero_rsp_code_raises_domain_error(monkeypatch):
    def fake_fetch(url: str, timeout_sec: float):
        return {"rsp_code": 7, "rsp_message": "failed", "data": []}

    monkeypatch.setattr(utils, "_fetch_json", fake_fetch)

    with pytest.raises(utils.OpenCtpDictError, match="rsp_code=\\[7\\]"):
        utils.get_markets()
