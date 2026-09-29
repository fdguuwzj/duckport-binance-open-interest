from __future__ import annotations

import json

import aiohttp

from binance_open_interest.config import proxy


class BinanceApiError(RuntimeError):
    def __init__(self, status: int, body: str, retry_after: str | None = None):
        super().__init__(f"Binance HTTP {status}: {body[:500]}")
        self.status = status
        self.body = body
        self.retry_after = retry_after


class BinanceOpenInterestRestApi:
    BASE_URL = "https://fapi.binance.com"

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def _get(self, path: str, params: dict | None = None):
        async with self.session.get(
            f"{self.BASE_URL}{path}",
            params=params or {},
            proxy=proxy,
        ) as response:
            body = await response.text()
            if not 200 <= response.status < 300:
                raise BinanceApiError(
                    response.status,
                    body,
                    response.headers.get("Retry-After"),
                )
            try:
                return json.loads(body)
            except ValueError as exc:
                raise RuntimeError(f"Invalid Binance JSON response: {body[:500]}") from exc

    async def exchange_info(self) -> dict:
        return await self._get("/fapi/v1/exchangeInfo")

    async def open_interest_history(self, **params) -> list[dict]:
        return await self._get("/futures/data/openInterestHist", params)

    async def open_interest(self, symbol: str) -> dict:
        return await self._get("/fapi/v1/openInterest", {"symbol": symbol})

    async def premium_index(self) -> list[dict]:
        payload = await self._get("/fapi/v1/premiumIndex")
        if isinstance(payload, dict):
            return [payload]
        return list(payload)
