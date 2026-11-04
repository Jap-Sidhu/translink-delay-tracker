"""Outage handling, with no network or database."""

import logging

import psycopg
import pytest
import requests

from conftest import TS, position_feed, trip_feed
from tracker.collector import Collector, MAX_BACKOFF_S, REFUSED_BACKOFF_S
from tracker.config import Config, ConfigError
from tracker.feed import FetchError, RedactingFormatter, fetch_feed, scrub
from tracker.store import Attempt

KEY = "SECRETKEY123"
CONFIG = Config(api_key=KEY)
GOOD = trip_feed([{"id": "T1", "route": "1", "stops": [(1, 30, TS + 60)]}])


class FakeStore:
    def __init__(self, used=0):
        self.used = used
        self.attempts = []
        self.saved = []
        self.down = False
        self.resets = 0

    def _check(self):
        if self.down:
            raise psycopg.OperationalError("connection refused")

    def requests_last_24h(self):
        self._check()
        return self.used

    def record_attempt(self, attempt: Attempt):
        self._check()
        self.attempts.append(attempt)

    def save_trip_updates(self, attempt, parsed, digest):
        self._check()
        self.saved.append(parsed)
        return True

    def save_positions(self, attempt, parsed, digest):
        self._check()
        self.saved.append(parsed)
        return True

    def reset(self):
        self.resets += 1


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def build(fetch, store=None):
    clock = Clock()
    store = store or FakeStore()
    collector = Collector(CONFIG, store, fetch, clock=clock)
    collector.feeds = collector.feeds[:1]  # Trip updates only.
    return collector, store, clock


def raising(error):
    def fetch(url):
        raise error
    return fetch


@pytest.mark.parametrize("status", ["timeout", "connection_error"])
def test_network_failure_is_recorded_and_survived(status):
    collector, store, _ = build(raising(FetchError(status, "boom")))
    collector.tick()
    assert [a.status for a in store.attempts] == [status]
    assert store.saved == []


def test_http_500_is_recorded_with_its_code():
    collector, store, _ = build(raising(FetchError("http_error", "HTTP 500", 500)))
    collector.tick()
    assert (store.attempts[0].status, store.attempts[0].http_status) == ("http_error", 500)


def test_garbage_body_is_a_parse_error_not_a_crash():
    collector, store, _ = build(lambda url: b"<html>gateway error</html>")
    collector.tick()
    assert store.attempts[0].status == "parse_error"
    assert store.saved == []


def test_repeated_failures_back_off_to_a_ceiling_then_recover():
    responses = [FetchError("timeout", "slow")] * 6 + [GOOD]

    def fetch(url):
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    collector, store, clock = build(fetch)
    state = collector.feeds[0]
    delays = []
    for _ in range(7):
        clock.now = state.next_due
        collector.tick()
        delays.append(state.next_due - clock.now)
    assert delays == [120, 240, 480, 960, MAX_BACKOFF_S, MAX_BACKOFF_S, 120]
    assert state.consecutive_failures == 0
    assert len(store.saved) == 1


@pytest.mark.parametrize("code", [401, 403, 429])
def test_refusal_backs_off_for_at_least_fifteen_minutes(code):
    collector, _, clock = build(raising(FetchError("http_error", f"HTTP {code}", code)))
    collector.tick()
    assert collector.feeds[0].next_due - clock.now == REFUSED_BACKOFF_S


def test_database_outage_spends_no_requests_and_recovers():
    calls = []

    def fetch(url):
        calls.append(url)
        return GOOD

    store = FakeStore()
    store.down = True
    collector, _, clock = build(fetch, store)
    collector.tick()
    assert calls == []
    assert store.resets == 1

    store.down = False
    clock.now = collector.feeds[0].next_due
    collector.tick()
    assert len(calls) == 1
    assert len(store.saved) == 1


def test_database_failure_during_save_does_not_crash():
    store = FakeStore()
    collector, _, _ = build(lambda url: GOOD, store)

    def fail(*args):
        raise psycopg.OperationalError("server closed the connection")

    store.save_trip_updates = fail
    collector.tick()
    assert store.resets == 1


def test_unexpected_error_does_not_stop_the_loop():
    collector, _, _ = build(raising(RuntimeError("surprise")))
    collector.tick()
    assert collector.feeds[0].consecutive_failures == 1


def test_route_refresh_failure_does_not_block_polling():
    def bad_refresh():
        raise requests.ConnectionError("static host down")

    collector, store, _ = build(lambda url: GOOD)
    collector.refresh_routes = bad_refresh
    collector.tick()
    assert len(store.saved) == 1


def test_budget_reached_makes_no_request():
    calls = []
    collector, store, _ = build(lambda url: calls.append(url) or GOOD, FakeStore(used=950))
    collector.tick()
    assert calls == []
    assert store.attempts[0].status == "skipped_budget"


def test_intervals_over_budget_are_rejected():
    with pytest.raises(ConfigError, match="requests per day"):
        Config(api_key=KEY, trip_poll_seconds=30).validate()


def test_budget_above_terms_limit_is_rejected():
    with pytest.raises(ConfigError, match="1000"):
        Config(api_key=KEY, daily_request_budget=1001).validate()


def test_default_schedule_fits_the_terms():
    assert CONFIG.requests_per_day() == 864
    CONFIG.validate()


def test_scrub_removes_key_by_value_and_by_parameter():
    text = f"GET /v3?apikey={KEY}&x=1 failed, key {KEY}; other apikey=abc"
    cleaned = scrub(text, KEY)
    assert KEY not in cleaned
    assert "abc" not in cleaned
    assert "x=1" in cleaned


class FailingSession:
    def __init__(self, error):
        self.error = error

    def get(self, url, **kwargs):
        raise self.error


@pytest.mark.parametrize(
    "error,status",
    [
        (requests.ConnectTimeout(f"timed out: /v3?apikey={KEY}"), "timeout"),
        (requests.ConnectionError(f"max retries: /v3?apikey={KEY}"), "connection_error"),
    ],
)
def test_real_request_errors_never_expose_the_key(error, status):
    with pytest.raises(FetchError) as caught:
        fetch_feed(FailingSession(error), "https://example.invalid/v3", KEY)
    assert caught.value.status == status
    assert KEY not in str(caught.value)
    assert caught.value.__suppress_context__ is True


def test_formatter_scrubs_tracebacks():
    formatter = RedactingFormatter("%(message)s", KEY)
    try:
        raise ValueError(f"bad url ?apikey={KEY}")
    except ValueError:
        import sys
        record = logging.LogRecord("t", logging.ERROR, __file__, 1, "fail", None, sys.exc_info())
    assert KEY not in formatter.format(record)
