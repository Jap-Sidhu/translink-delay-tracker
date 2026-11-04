"""The polling loop. Run it with: python -m tracker.collector"""

from __future__ import annotations

import hashlib
import logging
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import psycopg
import requests

from .config import (
    TRIP_UPDATES_URL,
    VEHICLE_POSITIONS_URL,
    Config,
    ConfigError,
    load_config,
)
from .feed import (
    FeedParseError,
    FetchError,
    RedactingFormatter,
    parse_positions,
    parse_trip_updates,
    scrub,
)
from .feed import fetch_feed as http_fetch_feed
from .static_gtfs import download_routes
from .store import Attempt, Store

log = logging.getLogger("tracker")

MAX_BACKOFF_S = 1800
# 401, 403 and 429 mean the server is refusing us. Hammering would make it worse.
REFUSED_BACKOFF_S = 900
DATABASE_RETRY_S = 30
STATIC_REFRESH_S = 86_400
STATIC_RETRY_S = 3_600


@dataclass
class FeedState:
    name: str
    url: str
    interval_s: int
    next_due: float = 0.0
    consecutive_failures: int = 0


class Collector:
    def __init__(
        self,
        config: Config,
        store,
        fetch: Callable[[str], bytes],
        refresh_routes: Callable[[], None] = lambda: None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.config = config
        self.store = store
        self.fetch = fetch
        self.refresh_routes = refresh_routes
        self.clock = clock
        self.feeds = [
            FeedState("trip_updates", TRIP_UPDATES_URL, config.trip_poll_seconds),
            FeedState("vehicle_positions", VEHICLE_POSITIONS_URL, config.position_poll_seconds),
        ]
        self.routes_due = 0.0

    def poll(self, state: FeedState) -> str:
        """Poll one feed once and return the snapshot status. Only database errors escape."""
        if self.store.requests_last_24h() >= self.config.daily_request_budget:
            log.warning("%s skipped: daily request budget reached", state.name)
            self.store.record_attempt(Attempt(state.name, "skipped_budget"))
            state.next_due = self.clock() + state.interval_s
            return "skipped_budget"

        started = self.clock()
        fetched_at = datetime.now(timezone.utc)
        attempt = Attempt(state.name, "ok", http_status=200)
        try:
            payload = self.fetch(state.url)
            attempt.duration_ms = round((self.clock() - started) * 1000)
            digest = hashlib.sha256(payload).hexdigest()
            if state.name == "trip_updates":
                parsed = parse_trip_updates(payload, fetched_at)
                written = self.store.save_trip_updates(attempt, parsed, digest)
            else:
                parsed = parse_positions(payload, fetched_at)
                written = self.store.save_positions(attempt, parsed, digest)
        except FetchError as exc:
            return self._failed(state, attempt, exc.status, str(exc), exc.http_status)
        except FeedParseError as exc:
            return self._failed(state, attempt, "parse_error", str(exc), attempt.http_status)

        state.consecutive_failures = 0
        state.next_due = self.clock() + state.interval_s
        status = "ok" if written else "duplicate"
        if parsed.skipped:
            log.warning("%s: %d malformed records skipped", state.name, parsed.skipped)
        log.info(
            "%s %s feed_ts=%s entities=%d", state.name, status,
            parsed.feed_ts.isoformat(), parsed.entity_count,
        )
        return status

    def _failed(self, state, attempt, status, detail, http_status) -> str:
        detail = scrub(detail, self.config.api_key)
        attempt.status, attempt.detail, attempt.http_status = status, detail, http_status
        self.store.record_attempt(attempt)
        state.consecutive_failures += 1
        delay = min(state.interval_s * 2 ** (state.consecutive_failures - 1), MAX_BACKOFF_S)
        if http_status in (401, 403, 429):
            delay = max(delay, REFUSED_BACKOFF_S)
        state.next_due = self.clock() + delay
        log.warning("%s %s: %s (next try in %ds)", state.name, status, detail, delay)
        return status

    def tick(self) -> None:
        """Run whatever is due. Nothing raised in here may stop the collector."""
        now = self.clock()
        if now >= self.routes_due:
            try:
                self.refresh_routes()
                self.routes_due = now + STATIC_REFRESH_S
            except Exception as exc:  # A names refresh must never block delay collection.
                self.routes_due = now + STATIC_RETRY_S
                self.store.reset()
                log.warning("route refresh failed: %s", scrub(str(exc), self.config.api_key))

        for state in self.feeds:
            if self.clock() < state.next_due:
                continue
            try:
                self.poll(state)
            except psycopg.Error as exc:
                # Without the database nothing can be stored or budgeted, so
                # spend no requests until it is back.
                self.store.reset()
                state.next_due = self.clock() + DATABASE_RETRY_S
                log.warning("%s: database unavailable: %s", state.name, exc)
            except Exception:
                self.store.reset()
                state.consecutive_failures += 1
                state.next_due = self.clock() + min(
                    state.interval_s * 2 ** (state.consecutive_failures - 1), MAX_BACKOFF_S
                )
                log.exception("%s: unexpected error", state.name)

    def seconds_until_due(self) -> float:
        return max(0.0, min(state.next_due for state in self.feeds) - self.clock())


def configure_logging(api_key: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(RedactingFormatter("%(asctime)s %(levelname)s %(message)s", api_key))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    # urllib3 logs request URLs at DEBUG. Keep that off even if the level changes.
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main() -> int:
    try:
        config = load_config()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    configure_logging(config.api_key)

    store = Store(config.database_url)
    session = requests.Session()
    last_modified: list[str | None] = [None]

    def refresh_routes() -> None:
        routes, last_modified[0] = download_routes(session, last_modified[0])
        if routes is not None:
            store.upsert_routes(routes)
            log.info("loaded %d routes from static GTFS", len(routes))

    collector = Collector(
        config,
        store,
        fetch=lambda url: http_fetch_feed(session, url, config.api_key),
        refresh_routes=refresh_routes,
    )

    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())

    log.info(
        "collector started: trip updates every %ds, positions every %ds, ~%.0f requests/day, budget %d",
        config.trip_poll_seconds, config.position_poll_seconds,
        config.requests_per_day(), config.daily_request_budget,
    )
    while not stop.is_set():
        collector.tick()
        stop.wait(max(collector.seconds_until_due(), 0.5))
    store.reset()
    log.info("collector stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
