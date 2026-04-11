"""
Utility helpers for querying the OpenCTP instrument dictionary service.
"""

from __future__ import annotations

from collections.abc import Iterable
import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

DEFAULT_DICT_BASE_URL = "http://dict.openctp.cn"
DEFAULT_TIMEOUT_SEC = 10.0


class OpenCtpDictError(RuntimeError):
    """
    Raise when the OpenCTP dictionary service returns a non-success response.

    :param message: Human-readable error message.
    :type message: str
    """


def get_markets(
    *,
    base_url: str = DEFAULT_DICT_BASE_URL,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
) -> list[dict[str, Any]]:
    """
    Query exchange metadata from the OpenCTP dictionary service.

    :param base_url: Root dictionary service URL.
    :type base_url: str
    :param timeout_sec: Request timeout in seconds.
    :type timeout_sec: float
    :returns: Exchange metadata rows.
    :rtype: list[dict[str, Any]]
    :raises OpenCtpDictError: If the service returns a non-zero response code.
    """
    return _request_data(
        "/markets",
        params={},
        base_url=base_url,
        timeout_sec=timeout_sec,
    )


def get_products(
    *,
    area: str | None = None,
    markets: str | Iterable[str] | None = None,
    type: str | None = None,
    products: str | Iterable[str] | None = None,
    base_url: str = DEFAULT_DICT_BASE_URL,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
) -> list[dict[str, Any]]:
    """
    Query product metadata from the OpenCTP dictionary service.

    :param area: Optional single area filter such as ``China``.
    :type area: str | None
    :param markets: Optional exchange filter, accepting a comma-separated string
        or an iterable of exchange identifiers.
    :type markets: str | Iterable[str] | None
    :param type: Optional single instrument-type filter such as ``futures``.
    :type type: str | None
    :param products: Optional product filter, accepting a comma-separated string
        or an iterable of product identifiers.
    :type products: str | Iterable[str] | None
    :param base_url: Root dictionary service URL.
    :type base_url: str
    :param timeout_sec: Request timeout in seconds.
    :type timeout_sec: float
    :returns: Product metadata rows.
    :rtype: list[dict[str, Any]]
    :raises OpenCtpDictError: If the service returns a non-zero response code.
    :raises ValueError: If multi-value filters resolve to an empty list.
    """
    return _request_data(
        "/products",
        params=_build_common_params(
            area=area,
            markets=markets,
            type=type,
            products=products,
        ),
        base_url=base_url,
        timeout_sec=timeout_sec,
    )


def get_instruments(
    *,
    area: str | None = None,
    markets: str | Iterable[str] | None = None,
    type: str | None = None,
    products: str | Iterable[str] | None = None,
    inst_life_phase: str | int | None = None,
    base_url: str = DEFAULT_DICT_BASE_URL,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
) -> list[dict[str, Any]]:
    """
    Query instrument metadata from the OpenCTP dictionary service.

    :param area: Optional single area filter.
    :type area: str | None
    :param markets: Optional exchange filter.
    :type markets: str | Iterable[str] | None
    :param type: Optional single instrument-type filter.
    :type type: str | None
    :param products: Optional product filter.
    :type products: str | Iterable[str] | None
    :param inst_life_phase: Optional instrument life-phase filter. This maps to
        the OpenCTP ``InstLifePhase`` query parameter.
    :type inst_life_phase: str | int | None
    :param base_url: Root dictionary service URL.
    :type base_url: str
    :param timeout_sec: Request timeout in seconds.
    :type timeout_sec: float
    :returns: Instrument metadata rows.
    :rtype: list[dict[str, Any]]
    :raises OpenCtpDictError: If the service returns a non-zero response code.
    :raises ValueError: If multi-value filters resolve to an empty list.
    """
    params = _build_common_params(
        area=area,
        markets=markets,
        type=type,
        products=products,
    )
    if inst_life_phase is not None:
        params["InstLifePhase"] = str(inst_life_phase)

    return _request_data(
        "/instruments",
        params=params,
        base_url=base_url,
        timeout_sec=timeout_sec,
    )


def get_quotes(
    *,
    area: str | None = None,
    markets: str | Iterable[str] | None = None,
    type: str | None = None,
    products: str | Iterable[str] | None = None,
    base_url: str = DEFAULT_DICT_BASE_URL,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
) -> list[dict[str, Any]]:
    """
    Query snapshot quote data from the OpenCTP dictionary service.

    :param area: Optional single area filter.
    :type area: str | None
    :param markets: Optional exchange filter.
    :type markets: str | Iterable[str] | None
    :param type: Optional single instrument-type filter.
    :type type: str | None
    :param products: Optional product filter.
    :type products: str | Iterable[str] | None
    :param base_url: Root dictionary service URL.
    :type base_url: str
    :param timeout_sec: Request timeout in seconds.
    :type timeout_sec: float
    :returns: Quote rows.
    :rtype: list[dict[str, Any]]
    :raises OpenCtpDictError: If the service returns a non-zero response code.
    :raises ValueError: If multi-value filters resolve to an empty list.
    """
    return _request_data(
        "/quotes",
        params=_build_common_params(
            area=area,
            markets=markets,
            type=type,
            products=products,
        ),
        base_url=base_url,
        timeout_sec=timeout_sec,
    )


def get_sessions(
    *,
    area: str | None = None,
    markets: str | Iterable[str] | None = None,
    type: str | None = None,
    products: str | Iterable[str] | None = None,
    base_url: str = DEFAULT_DICT_BASE_URL,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
) -> list[dict[str, Any]]:
    """
    Query trading-session segments from the OpenCTP dictionary service.

    :param area: Optional single area filter.
    :type area: str | None
    :param markets: Optional exchange filter.
    :type markets: str | Iterable[str] | None
    :param type: Optional single instrument-type filter.
    :type type: str | None
    :param products: Optional product filter.
    :type products: str | Iterable[str] | None
    :param base_url: Root dictionary service URL.
    :type base_url: str
    :param timeout_sec: Request timeout in seconds.
    :type timeout_sec: float
    :returns: Trading-session rows.
    :rtype: list[dict[str, Any]]
    :raises OpenCtpDictError: If the service returns a non-zero response code.
    :raises ValueError: If multi-value filters resolve to an empty list.
    """
    return _request_data(
        "/sessions",
        params=_build_common_params(
            area=area,
            markets=markets,
            type=type,
            products=products,
        ),
        base_url=base_url,
        timeout_sec=timeout_sec,
    )


def _build_common_params(
    *,
    area: str | None,
    markets: str | Iterable[str] | None,
    type: str | None,
    products: str | Iterable[str] | None,
) -> dict[str, str]:
    """
    Build normalized query parameters shared by most dictionary endpoints.

    :param area: Optional single area filter.
    :type area: str | None
    :param markets: Optional exchange filter.
    :type markets: str | Iterable[str] | None
    :param type: Optional instrument-type filter.
    :type type: str | None
    :param products: Optional product filter.
    :type products: str | Iterable[str] | None
    :returns: Normalized query parameter mapping.
    :rtype: dict[str, str]
    """
    params: dict[str, str] = {}
    if area is not None:
        params["area"] = _normalize_single(area, "area")
    if markets is not None:
        params["markets"] = _normalize_multi(markets, "markets")
    if type is not None:
        params["type"] = _normalize_single(type, "type")
    if products is not None:
        params["products"] = _normalize_multi(products, "products")
    return params


def _normalize_single(value: str, name: str) -> str:
    """
    Normalize a required single-value query parameter.

    :param value: Raw query value.
    :type value: str
    :param name: Parameter name for validation errors.
    :type name: str
    :returns: Trimmed query value.
    :rtype: str
    :raises ValueError: If the value is empty after trimming.
    """
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _normalize_multi(value: str | Iterable[str], name: str) -> str:
    """
    Normalize a multi-value query parameter into the OpenCTP comma format.

    :param value: Raw multi-value input.
    :type value: str | Iterable[str]
    :param name: Parameter name for validation errors.
    :type name: str
    :returns: Comma-separated value string.
    :rtype: str
    :raises ValueError: If no non-empty values remain after trimming.
    """
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
    else:
        items = [str(item).strip() for item in value if str(item).strip()]

    if not items:
        raise ValueError(f"{name} must not resolve to an empty value list")

    return ",".join(items)


def _request_data(
    path: str,
    *,
    params: dict[str, str],
    base_url: str,
    timeout_sec: float,
) -> list[dict[str, Any]]:
    """
    Execute a dictionary request and extract the ``data`` field.

    :param path: Endpoint path such as ``/markets``.
    :type path: str
    :param params: Query parameter mapping.
    :type params: dict[str, str]
    :param base_url: Root dictionary service URL.
    :type base_url: str
    :param timeout_sec: Request timeout in seconds.
    :type timeout_sec: float
    :returns: Response ``data`` field.
    :rtype: list[dict[str, Any]]
    :raises OpenCtpDictError: If the service returns a non-zero response code.
    """
    base_url = base_url.rstrip("/")
    query = urlencode(params)
    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{query}"

    payload = _fetch_json(url, timeout_sec)
    rsp_code = payload.get("rsp_code")
    if rsp_code != 0:
        raise OpenCtpDictError(
            "openctp dict request failed "
            f"url=[{url}] rsp_code=[{rsp_code}] rsp_message=[{payload.get('rsp_message')}]"
        )

    data = payload.get("data", [])
    if not isinstance(data, list):
        raise OpenCtpDictError(f"openctp dict response data must be a list url=[{url}]")
    return data


def _fetch_json(url: str, timeout_sec: float) -> dict[str, Any]:
    """
    Fetch a JSON document from the given URL.

    :param url: Fully-qualified request URL.
    :type url: str
    :param timeout_sec: Request timeout in seconds.
    :type timeout_sec: float
    :returns: Parsed JSON payload.
    :rtype: dict[str, Any]
    """
    with urlopen(url, timeout=timeout_sec) as response:
        return json.load(response)
