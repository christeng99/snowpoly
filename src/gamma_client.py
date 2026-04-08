"""
Gamma API Client - Market Discovery for Polymarket

Provides access to the Gamma API for discovering active markets,
including 5-minute Up/Down markets for crypto assets.

Example:
    from src.gamma_client import GammaClient

    client = GammaClient()
    market = client.get_current_5m_market("ETH")
    print(market["slug"], market["clobTokenIds"])
"""

import json
import time
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone

from .http import ThreadLocalSessionMixin

# Polymarket crypto up/down “5m” markets use a 300s slug timestamp grid.
UP_DOWN_WINDOW_SEC = 300


class GammaClient(ThreadLocalSessionMixin):
    """
    Client for Polymarket's Gamma API.

    Used to discover markets and get market metadata.
    """

    DEFAULT_HOST = "https://gamma-api.polymarket.com"

    # Supported coins and their slug prefixes
    COIN_SLUGS = {
        "BTC": "btc-updown-5m",
        "ETH": "eth-updown-5m",
        "SOL": "sol-updown-5m",
        "XRP": "xrp-updown-5m",
    }

    def __init__(self, host: str = DEFAULT_HOST, timeout: int = 10):
        """
        Initialize Gamma client.

        Args:
            host: Gamma API host URL
            timeout: Request timeout in seconds
        """
        super().__init__()
        self.host = host.rstrip("/")
        self.timeout = timeout

    def get_market_by_slug(self, slug: str) -> Optional[Dict[str, Any]]:
        """
        Get market data by slug.

        Args:
            slug: Market slug (e.g., "eth-updown-5m-1766671200")

        Returns:
            Market data dictionary or None if not found
        """
        url = f"{self.host}/markets/slug/{slug}"

        try:
            response = self.session.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception:
            return None

    def get_current_5m_market(self, coin: str) -> Optional[Dict[str, Any]]:
        """
        Get the current active 5-minute market for a coin.

        Uses epoch-aligned 300s slug timestamps (matching Gamma) and probes a
        wider set of neighbors than (+/- one window). That avoids sticking to a
        stale slug or skipping a window around boundaries — which showed up as
        600s steps in stored round_ts instead of 300s.

        Args:
            coin: Coin symbol (BTC, ETH, SOL, XRP)

        Returns:
            Market data for the current 5-minute window, or None
        """
        coin = coin.upper()
        if coin not in self.COIN_SLUGS:
            raise ValueError(f"Unsupported coin: {coin}. Use: {list(self.COIN_SLUGS.keys())}")

        prefix = self.COIN_SLUGS[coin]
        now_ts = int(time.time())
        t0 = (now_ts // UP_DOWN_WINDOW_SEC) * UP_DOWN_WINDOW_SEC

        # Order: current window first, then near neighbors, then ±2 windows.
        w = UP_DOWN_WINDOW_SEC
        deltas = (0, w, -w, 2 * w, -2 * w, 3 * w, -3 * w)
        accepting: List[Tuple[int, Dict[str, Any]]] = []

        for delta in deltas:
            ts = t0 + delta
            slug = f"{prefix}-{ts}"
            market = self.get_market_by_slug(slug)
            if not market or not market.get("acceptingOrders"):
                continue
            if ts <= now_ts < ts + w:
                return market
            accepting.append((ts, market))

        if not accepting:
            return None

        accepting.sort(key=lambda item: abs(item[0] - t0))
        return accepting[0][1]

    def get_next_5m_market(self, coin: str) -> Optional[Dict[str, Any]]:
        """
        Get the next upcoming 5-minute market for a coin.

        Args:
            coin: Coin symbol (BTC, ETH, SOL, XRP)

        Returns:
            Market data for the next 5-minute window, or None
        """
        coin = coin.upper()
        if coin not in self.COIN_SLUGS:
            raise ValueError(f"Unsupported coin: {coin}")

        prefix = self.COIN_SLUGS[coin]
        now_ts = int(time.time())
        next_ts = (now_ts // UP_DOWN_WINDOW_SEC + 1) * UP_DOWN_WINDOW_SEC
        slug = f"{prefix}-{next_ts}"

        return self.get_market_by_slug(slug)

    def parse_token_ids(self, market: Dict[str, Any]) -> Dict[str, str]:
        """
        Parse token IDs from market data.

        Args:
            market: Market data dictionary

        Returns:
            Dictionary with "up" and "down" token IDs
        """
        clob_token_ids = market.get("clobTokenIds", "[]")
        token_ids = self._parse_json_field(clob_token_ids)

        outcomes = market.get("outcomes", '["Up", "Down"]')
        outcomes = self._parse_json_field(outcomes)

        return self._map_outcomes(outcomes, token_ids)

    def parse_prices(self, market: Dict[str, Any]) -> Dict[str, float]:
        """
        Parse current prices from market data.

        Args:
            market: Market data dictionary

        Returns:
            Dictionary with "up" and "down" prices
        """
        outcome_prices = market.get("outcomePrices", '["0.5", "0.5"]')
        prices = self._parse_json_field(outcome_prices)

        outcomes = market.get("outcomes", '["Up", "Down"]')
        outcomes = self._parse_json_field(outcomes)

        return self._map_outcomes(outcomes, prices, cast=float)

    @staticmethod
    def _parse_json_field(value: Any) -> List[Any]:
        """Parse a field that may be a JSON string or a list."""
        if isinstance(value, str):
            return json.loads(value)
        return value

    @staticmethod
    def _map_outcomes(
        outcomes: List[Any],
        values: List[Any],
        cast=lambda v: v
    ) -> Dict[str, Any]:
        """Map outcome labels to values with optional casting."""
        result: Dict[str, Any] = {}
        for i, outcome in enumerate(outcomes):
            if i < len(values):
                result[str(outcome).lower()] = cast(values[i])
        return result

    def get_market_info(self, coin: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive market info for current 5-minute market.

        Args:
            coin: Coin symbol

        Returns:
            Dictionary with market info including token IDs and prices
        """
        market = self.get_current_5m_market(coin)
        if not market:
            return None

        token_ids = self.parse_token_ids(market)
        prices = self.parse_prices(market)

        return {
            "slug": market.get("slug"),
            "question": market.get("question"),
            "end_date": market.get("endDate"),
            "token_ids": token_ids,
            "prices": prices,
            "accepting_orders": market.get("acceptingOrders", False),
            "best_bid": market.get("bestBid"),
            "best_ask": market.get("bestAsk"),
            "spread": market.get("spread"),
            "raw": market,
        }
