"""
Real-time Polymarket up/down best bid / best ask for BTC, ETH, SOL, XRP — persisted to SQLite.

Database path: ``{DATA_DIR}/prices_{YYYY-MM-DD}.db`` (UTC date; env ``DATA_DIR``, default ``data``).

Tables: btc_up, btc_down, eth_up, eth_down, sol_up, sol_down, xrp_up, xrp_down
Columns: round_ts (epoch sec, window start from market slug), msecs (ms since that start),
best_bid, best_ask, mid (same rules as ``OrderbookSnapshot.mid_price``; 0 means empty / no quote).

Only rows with msecs in ``[0, 299_000]`` are stored (5-minute windows; keeps prices aligned with the active round and drops end-of-window edge samples).

Buffers samples in memory per table; when a coin's market slug/round changes, that coin's buffers are flushed.

Consecutive book updates where best_bid, best_ask, and mid are all unchanged at **0.01** precision are not appended.

Existing DBs are migrated: legacy ``price`` becomes bid/ask/mid; older bid/ask-only tables get a ``mid`` column backfilled from the book formula.

Run from repo root::

    python -m snowpoly.realtime_prices

Optional: ``pip install python-dotenv`` and set ``DATA_DIR`` in ``.env``.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import logging
import os
import sqlite3
import sys
import time
import types
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
except ImportError:
    pass


def _load_src_module(rel_path: str, full_name: str) -> Any:
    """Load a file under src/ without importing src/__init__.py (avoids extra deps)."""
    path = _ROOT / "src" / rel_path
    spec = importlib.util.spec_from_file_location(full_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {full_name} from {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[full_name] = mod
    spec.loader.exec_module(mod)
    return mod


def _ensure_src_submodules() -> None:
    if "src" not in sys.modules:
        src_pkg = types.ModuleType("src")
        src_pkg.__path__ = [str(_ROOT / "src")]  # type: ignore[attr-defined]
        sys.modules["src"] = src_pkg
    if "src.http" not in sys.modules:
        _load_src_module("http.py", "src.http")
    if "src.gamma_client" not in sys.modules:
        _load_src_module("gamma_client.py", "src.gamma_client")
    if "src.websocket_client" not in sys.modules:
        _load_src_module("websocket_client.py", "src.websocket_client")


_ensure_src_submodules()
GammaClient = sys.modules["src.gamma_client"].GammaClient
MarketWebSocket = sys.modules["src.websocket_client"].MarketWebSocket
OrderbookSnapshot = sys.modules["src.websocket_client"].OrderbookSnapshot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("snowpoly.realtime_prices")

COINS = ("BTC", "ETH", "SOL", "XRP")
COIN_TABLE_PREFIX = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "XRP": "xrp"}

Row = Tuple[int, float, float, float, float]  # round_ts, msecs, best_bid, best_ask, mid

# 5-minute Gamma windows are 300_000 ms; persist [0, 299_000] ms from round start.
MSECS_CAPTURE_MAX = 299_000

# Dedupe sequential bid/ask to 0.01 (Polymarket-style tick).
_PRICE_TICK_DECIMALS = 2


def _same_price_tick(a: float, b: float) -> bool:
    return round(a, _PRICE_TICK_DECIMALS) == round(b, _PRICE_TICK_DECIMALS)


def _msecs_in_capture_window(msecs: float) -> bool:
    return 0.0 <= msecs <= float(MSECS_CAPTURE_MAX)


def _slug_round_ts(slug: str) -> Optional[int]:
    if not slug:
        return None
    tail = slug.rsplit("-", 1)[-1]
    return int(tail) if tail.isdigit() else None


def _data_dir() -> Path:
    raw = os.environ.get("DATA_DIR", "data")
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = _ROOT / p
    p.mkdir(parents=True, exist_ok=True)
    return p


def _db_path_for_utc_today() -> Path:
    day = datetime.now(timezone.utc).date().isoformat()
    return _data_dir() / f"prices_{day}.db"


def _table_column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {str(row[1]) for row in cur.fetchall()}


def _rebuild_table_without_legacy_price(conn: sqlite3.Connection, name: str) -> None:
    """Replace table that still has ``price`` with bid/ask/mid schema; preserve rows."""
    tmp = f"{name}_migrate_tmp"
    conn.execute(f"DROP TABLE IF EXISTS {tmp}")
    conn.execute(
        f"""
        CREATE TABLE {tmp} (
            round_ts INTEGER NOT NULL,
            msecs REAL NOT NULL,
            best_bid REAL NOT NULL,
            best_ask REAL NOT NULL,
            mid REAL NOT NULL
        )
        """
    )
    # Mid-only era: copy ``price`` into bid, ask, and mid. Rows with real bid/ask keep legs; mid from ``price``.
    conn.execute(
        f"""
        INSERT INTO {tmp} (round_ts, msecs, best_bid, best_ask, mid)
        SELECT round_ts, msecs,
            CASE WHEN best_bid > 0 THEN best_bid ELSE price END,
            CASE WHEN best_ask > 0 THEN best_ask ELSE price END,
            price
        FROM {name}
        """
    )
    conn.execute(f"DROP TABLE {name}")
    conn.execute(f"ALTER TABLE {tmp} RENAME TO {name}")


def _backfill_mid_column(conn: sqlite3.Connection, name: str) -> None:
    """Set ``mid`` from bid/ask for rows where it was defaulted (migration)."""
    conn.execute(
        f"""
        UPDATE {name} SET mid = CASE
            WHEN best_bid > 0 AND best_ask > 0 THEN (best_bid + best_ask) / 2
            WHEN best_bid > 0 THEN best_bid
            WHEN best_ask > 0 THEN best_ask
            ELSE 0
        END
        """
    )


def _ensure_schema(conn: sqlite3.Connection) -> None:
    for coin in COINS:
        prefix = COIN_TABLE_PREFIX[coin]
        for side in ("up", "down"):
            name = f"{prefix}_{side}"
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {name} (
                    round_ts INTEGER NOT NULL,
                    msecs REAL NOT NULL,
                    best_bid REAL NOT NULL,
                    best_ask REAL NOT NULL,
                    mid REAL NOT NULL
                )
                """
            )
            cols = _table_column_names(conn, name)
            if "best_bid" not in cols:
                conn.execute(
                    f"ALTER TABLE {name} ADD COLUMN best_bid REAL NOT NULL DEFAULT 0"
                )
            if "best_ask" not in cols:
                conn.execute(
                    f"ALTER TABLE {name} ADD COLUMN best_ask REAL NOT NULL DEFAULT 0"
                )
            cols = _table_column_names(conn, name)
            if "price" in cols:
                _rebuild_table_without_legacy_price(conn, name)
                cols = _table_column_names(conn, name)
            if "mid" not in cols:
                conn.execute(
                    f"ALTER TABLE {name} ADD COLUMN mid REAL NOT NULL DEFAULT 0"
                )
                _backfill_mid_column(conn, name)
    conn.commit()


def _insert_rows(conn: sqlite3.Connection, table: str, rows: List[Row]) -> None:
    rows = [r for r in rows if _msecs_in_capture_window(r[1])]
    if not rows:
        return
    conn.executemany(
        f"INSERT INTO {table} (round_ts, msecs, best_bid, best_ask, mid) VALUES (?, ?, ?, ?, ?)",
        rows,
    )


@dataclass
class CaptureState:
    gamma: GammaClient = field(default_factory=GammaClient)
    ws: Optional[MarketWebSocket] = None
    # coin -> slug
    slugs: Dict[str, str] = field(default_factory=dict)
    # coin -> window start epoch seconds (from slug)
    round_starts: Dict[str, int] = field(default_factory=dict)
    # coin -> {up: token_id, down: token_id}
    coin_tokens: Dict[str, Dict[str, str]] = field(default_factory=dict)
    token_route: Dict[str, Tuple[str, str]] = field(default_factory=dict)
    # table_name -> buffered rows
    buffers: Dict[str, List[Row]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    poll_interval: float = 10.0
    _last_subscribed: Optional[Tuple[str, ...]] = None

    def __post_init__(self) -> None:
        for coin in COINS:
            p = COIN_TABLE_PREFIX[coin]
            self.buffers[f"{p}_up"] = []
            self.buffers[f"{p}_down"] = []

    def _table(self, coin: str, side: str) -> str:
        return f"{COIN_TABLE_PREFIX[coin]}_{side}"

    def _apply_discovery(
        self, per_coin: Dict[str, Optional[Dict[str, Any]]]
    ) -> Tuple[List[str], bool]:
        """
        Update slug/round/token state from discovery results.
        Returns (asset_ids for subscribe, subscriptions_changed).
        """
        asset_ids: List[str] = []
        routes: Dict[str, Tuple[str, str]] = {}
        changed_tokens = False

        for coin in COINS:
            info = per_coin.get(coin)
            if not info:
                ct = self.coin_tokens.get(coin)
                if ct:
                    for side in ("up", "down"):
                        tid = ct.get(side)
                        if tid:
                            asset_ids.append(tid)
                            routes[tid] = (coin, side)
                continue

            slug = info.get("slug") or ""
            tids = info.get("token_ids") or {}
            new_up = tids.get("up", "")
            new_down = tids.get("down", "")

            old = self.coin_tokens.get(coin, {})
            if new_up != old.get("up") or new_down != old.get("down"):
                changed_tokens = True

            self.slugs[coin] = slug
            rs = _slug_round_ts(slug)
            if rs is not None:
                self.round_starts[coin] = rs
            self.coin_tokens[coin] = {"up": new_up, "down": new_down}

            for side in ("up", "down"):
                tid = tids.get(side, "")
                if tid:
                    asset_ids.append(tid)
                    routes[tid] = (coin, side)

        self.token_route = routes
        return asset_ids, changed_tokens

    async def _flush_coin_rows(
        self, coin: str, up_rows: List[Row], down_rows: List[Row]
    ) -> None:
        if not up_rows and not down_rows:
            return
        path = _db_path_for_utc_today()
        up_t = self._table(coin, "up")
        down_t = self._table(coin, "down")

        def _write() -> None:
            conn = sqlite3.connect(str(path))
            try:
                _ensure_schema(conn)
                _insert_rows(conn, up_t, up_rows)
                _insert_rows(conn, down_t, down_rows)
                conn.commit()
            finally:
                conn.close()

        await asyncio.to_thread(_write)
        logger.info(
            "Flushed %s (%d + %d rows) -> %s",
            coin,
            len(up_rows),
            len(down_rows),
            path,
        )

    async def on_book(self, snapshot: OrderbookSnapshot) -> None:
        tid = snapshot.asset_id
        async with self.lock:
            route = self.token_route.get(tid)
            if not route:
                return
            coin, side = route
            rs = self.round_starts.get(coin)
            if rs is None:
                return
            msecs = (time.time() - rs) * 1000.0
            if not _msecs_in_capture_window(msecs):
                return
            bid = snapshot.best_bid
            ask = snapshot.best_ask
            mid = snapshot.mid_price
            if bid <= 0 and ask <= 0:
                return
            table = self._table(coin, side)
            buf = self.buffers[table]
            if buf and buf[-1][0] == rs:
                _, _, lb, la, lm = buf[-1]
                if (
                    _same_price_tick(lb, bid)
                    and _same_price_tick(la, ask)
                    and _same_price_tick(lm, mid)
                ):
                    return
            buf.append((rs, msecs, bid, ask, mid))

    async def discover_parallel(self) -> Dict[str, Optional[Dict[str, Any]]]:
        def _one(c: str) -> Tuple[str, Optional[Dict[str, Any]]]:
            return c, self.gamma.get_market_info(c)

        results = await asyncio.gather(
            *[asyncio.to_thread(_one, c) for c in COINS],
        )
        return {c: info for c, info in results}

    async def poll_markets(self) -> None:
        while True:
            await asyncio.sleep(self.poll_interval)
            per = await self.discover_parallel()
            pending: List[Tuple[str, List[Row], List[Row]]] = []
            asset_ids: List[str] = []

            async with self.lock:
                changed_slugs: List[str] = []
                for coin in COINS:
                    info = per.get(coin)
                    if not info:
                        continue
                    new_slug = info.get("slug") or ""
                    old_slug = self.slugs.get(coin)
                    if old_slug is not None and new_slug and new_slug != old_slug:
                        changed_slugs.append(coin)

                for coin in changed_slugs:
                    up_t = self._table(coin, "up")
                    down_t = self._table(coin, "down")
                    pending.append((coin, list(self.buffers[up_t]), list(self.buffers[down_t])))
                    self.buffers[up_t].clear()
                    self.buffers[down_t].clear()

                asset_ids, _ = self._apply_discovery(per)
                new_key = tuple(sorted(asset_ids)) if asset_ids else ()
                prev_key = self._last_subscribed or ()
                should_resub = bool(asset_ids) and new_key != prev_key

            for coin, up_rows, down_rows in pending:
                await self._flush_coin_rows(coin, up_rows, down_rows)

            if self.ws and should_resub:
                await self.ws.subscribe(asset_ids, replace=True)
                async with self.lock:
                    self._last_subscribed = tuple(sorted(asset_ids))
                logger.info("Resubscribed to %d assets", len(asset_ids))

    async def run(self) -> None:
        per = await self.discover_parallel()
        if not any(per.values()):
            logger.error("No active markets from Gamma; exiting.")
            return

        async with self.lock:
            asset_ids, _ = self._apply_discovery(per)
        if not asset_ids:
            logger.error("No token IDs discovered; exiting.")
            return

        self.ws = MarketWebSocket()

        @self.ws.on_book
        async def _book(s: OrderbookSnapshot) -> None:  # type: ignore[unused-ignore]
            await self.on_book(s)

        @self.ws.on_connect
        def _conn() -> None:
            logger.info("WebSocket connected")

        @self.ws.on_disconnect
        def _disc() -> None:
            logger.warning("WebSocket disconnected")

        poll_task = asyncio.create_task(self.poll_markets())

        try:
            if not await self.ws.connect():
                return
            await self.ws.subscribe(asset_ids, replace=True)
            self._last_subscribed = tuple(sorted(asset_ids))
            logger.info(
                "Subscribed to %d assets; writing to %s",
                len(asset_ids),
                _db_path_for_utc_today(),
            )
            await self.ws.run(auto_reconnect=True)
        finally:
            poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await poll_task
            if self.ws:
                await self.ws.disconnect()
            pending_final: List[Tuple[str, List[Row], List[Row]]] = []
            async with self.lock:
                for coin in COINS:
                    up_t = self._table(coin, "up")
                    down_t = self._table(coin, "down")
                    pending_final.append(
                        (coin, list(self.buffers[up_t]), list(self.buffers[down_t]))
                    )
                    self.buffers[up_t].clear()
                    self.buffers[down_t].clear()
            for coin, up_rows, down_rows in pending_final:
                await self._flush_coin_rows(coin, up_rows, down_rows)


def main() -> None:
    state = CaptureState()
    try:
        asyncio.run(state.run())
    except KeyboardInterrupt:
        logger.info("Stopped.")


if __name__ == "__main__":
    main()
