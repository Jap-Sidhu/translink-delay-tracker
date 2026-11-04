"""Run against real Postgres. Skipped unless TEST_DATABASE_URL is set."""

import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest

from conftest import TS, at, position_feed, trip_feed
from google.transit import gtfs_realtime_pb2 as pb
from tracker.feed import parse_positions, parse_trip_updates
from tracker.store import Attempt, Store

URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="TEST_DATABASE_URL not set")
SQL = Path(__file__).parent.parent / "sql"
FETCHED = at(TS + 5)


@pytest.fixture
def store():
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    instance = Store(URL)
    yield instance
    instance.reset()


def save(store, trips, ts=TS, sha=None):
    parsed = parse_trip_updates(trip_feed(trips, ts=ts), FETCHED)
    return store.save_trip_updates(Attempt("trip_updates", "ok"), parsed, sha or f"sha{ts}")


def one(store, sql):
    return store.connection().execute(sql).fetchone()


def all_rows(store, sql, params=None):
    return store.connection().execute(sql, params).fetchall()


def test_duplicate_snapshot_is_not_double_counted(store):
    trip = [{"id": "T1", "route": "1", "stops": [(1, 30, TS + 60)]}]
    assert save(store, trip) is True
    assert save(store, trip) is False
    assert one(store, "SELECT count(*) FROM stop_delays") == (1,)
    statuses = dict(all_rows(store, "SELECT status, count(*) FROM snapshots GROUP BY 1"))
    assert statuses == {"ok": 1, "duplicate": 1}


def test_older_feed_is_ignored(store):
    save(store, [{"id": "T1", "route": "1", "stops": [(1, 30, TS + 60)]}], ts=TS)
    assert save(store, [{"id": "T1", "route": "1", "stops": [(1, 99, TS + 60)]}], ts=TS - 60) is False
    assert one(store, "SELECT arrival_delay_s FROM stop_delays") == (30,)


def test_identical_content_under_a_new_timestamp_is_a_duplicate(store):
    save(store, [{"id": "T1", "route": "1", "stops": [(1, 30, TS + 60)]}], ts=TS, sha="same")
    assert save(store, [{"id": "T1", "route": "1", "stops": [(1, 30, TS + 60)]}], ts=TS + 30, sha="same") is False


def test_failed_write_leaves_no_snapshot(store):
    trips = [{"id": "T1", "route": "1", "stops": [(1, 30, TS + 60)]}]
    parsed = parse_trip_updates(trip_feed(trips), FETCHED)
    parsed.stops[0]["trip_id"] = "no-such-trip"  # Breaks the foreign key mid-transaction.
    with pytest.raises(psycopg.Error):
        store.save_trip_updates(Attempt("trip_updates", "ok"), parsed, "x")
    assert one(store, "SELECT count(*) FROM snapshots") == (0,)
    assert one(store, "SELECT count(*) FROM trips") == (0,)


def test_row_keeps_first_and_last_prediction(store):
    save(store, [{"id": "T1", "route": "1", "stops": [(1, 30, TS + 300)]}], ts=TS)
    save(store, [{"id": "T1", "route": "1", "stops": [(1, 95, TS + 300)]}], ts=TS + 120)
    row = one(store, "SELECT first_arrival_delay_s, arrival_delay_s, last_horizon_s FROM stop_delays")
    assert row == (30, 95, 180)


def test_position_rows_are_not_duplicated(store):
    parsed = parse_positions(position_feed([("V1", TS - 30)]), FETCHED)
    store.save_positions(Attempt("vehicle_positions", "ok"), parsed, "a")
    parsed = parse_positions(position_feed([("V1", TS - 30)], ts=TS + 600), FETCHED)
    store.save_positions(Attempt("vehicle_positions", "ok"), parsed, "b")
    assert one(store, "SELECT count(*) FROM vehicle_positions") == (1,)


def observed(store):
    return all_rows(
        store,
        "SELECT trip_id, stop_sequence, arrival_delay_s, route_label FROM observed_stop_delays ORDER BY 1, 2",
    )


def test_delay_is_the_last_prediction_once_the_stop_leaves_the_feed(store):
    store.upsert_routes([{"route_id": "1", "short_name": "099", "long_name": "UBC", "route_type": 3}])
    save(store, [{"id": "T1", "route": "1", "stops": [(1, 30, TS + 60), (2, 40, TS + 180)]}], ts=TS)
    assert observed(store) == []
    save(store, [{"id": "T1", "route": "1", "stops": [(1, 45, TS + 90), (2, 50, TS + 190)]}], ts=TS + 60)
    save(store, [{"id": "T1", "route": "1", "stops": [(2, 55, TS + 195)]}], ts=TS + 120)
    # Stop 1 is gone, so its last prediction counts. Stop 2 is still upcoming.
    assert observed(store) == [("T1", 1, 45, "099 UBC")]


def test_canceled_trips_stay_out_of_delays_but_are_kept(store):
    save(store, [{"id": "T1", "route": "1", "stops": [(1, 30, TS + 60)]}], ts=TS)
    save(store, [{"id": "T1", "route": "1", "relationship": pb.TripDescriptor.CANCELED}], ts=TS + 120)
    save(store, [{"id": "T2", "route": "1", "stops": [(1, 0, TS + 300)]}], ts=TS + 240)
    assert observed(store) == []
    assert one(store, "SELECT count(*) FROM trips WHERE ever_canceled") == (1,)
    assert all_rows(store, (SQL / "cancellations.sql").read_text())[0][1:3] == (1, 1)


def test_outliers_and_stale_forecasts_are_excluded_but_stored(store):
    save(store, [{"id": "T1", "route": "1", "stops": [
        (1, -4280, TS + 60),  # Outlier.
        (2, 20, TS + 900),    # Forecast too far ahead to count as observed.
        (3, 3600, TS + 120),  # Very late, kept.
    ]}], ts=TS)
    save(store, [{"id": "T2", "route": "1", "stops": [(1, 0, TS + 500)]}], ts=TS + 120)
    assert [r[1] for r in observed(store)] == [3]
    assert one(store, "SELECT count(*) FROM stop_delays") == (4,)
    assert one(store, "SELECT count(*) FROM stop_delay_candidates WHERE is_outlier") == (1,)


def test_unmatched_and_blank_routes_are_labelled_not_dropped(store):
    save(store, [
        {"id": "T1", "route": "404", "stops": [(1, 10, TS + 60)]},
        {"id": "T2", "stops": [(1, 20, TS + 60)]},
    ], ts=TS)
    save(store, [{"id": "T3", "route": "404", "stops": [(1, 0, TS + 500)]}], ts=TS + 120)
    assert [r[3] for r in observed(store)] == ["Unmatched route 404", "Unmatched route (no id)"]


def test_delay_query_groups_by_route_and_local_hour(store):
    store.upsert_routes([{"route_id": "1", "short_name": "099", "long_name": "UBC", "route_type": 3}])
    # 23:37 UTC in July is 16:37 in Vancouver, and scheduled time is 2 minutes later minus delay.
    ts = int(datetime(2026, 7, 15, 23, 37, tzinfo=timezone.utc).timestamp())
    save(store, [
        {"id": "T1", "route": "1", "stops": [(1, 60, ts + 120)]},
        {"id": "T2", "route": "1", "stops": [(1, 120, ts + 120)]},
    ], ts=ts)
    save(store, [{"id": "T3", "route": "1", "stops": [(1, 0, ts + 500)]}], ts=ts + 120)
    surround_with_polls(store, ts)
    rows = all_rows(store, (SQL / "delay_by_route_hour.sql").read_text())
    assert rows == [("099 UBC", 16, 90, 90, 114, 2, 2)]


def surround_with_polls(store, ts, skip=None):
    """Add two hours of steady snapshots either side of ts, minus any skipped span."""
    for offset in range(-7199, 7200, 120):
        if skip and skip[0] <= offset <= skip[1]:
            continue
        store.connection().execute(
            "INSERT INTO snapshots (feed, status, feed_ts) VALUES ('trip_updates', 'ok', %s)",
            (at(ts + offset),),
        )


def settled(store):
    return [r[0] for r in all_rows(store, "SELECT trip_id FROM settled_stop_delays ORDER BY 1")]


def test_stops_near_the_edges_of_collection_are_not_settled(store):
    save(store, [{"id": "T1", "route": "1", "stops": [(1, -60, TS + 60)]}], ts=TS)
    save(store, [{"id": "T2", "route": "1", "stops": [(1, 0, TS + 500)]}], ts=TS + 120)
    assert len(observed(store)) == 1
    assert settled(store) == []
    surround_with_polls(store, TS)
    assert settled(store) == ["T1"]


def test_stops_near_an_outage_are_not_settled(store):
    save(store, [{"id": "T1", "route": "1", "stops": [(1, -60, TS + 60)]}], ts=TS)
    save(store, [{"id": "T2", "route": "1", "stops": [(1, 0, TS + 500)]}], ts=TS + 120)
    surround_with_polls(store, TS, skip=(600, 1800))
    assert settled(store) == []


def test_one_missed_poll_does_not_count_as_an_outage(store):
    save(store, [{"id": "T1", "route": "1", "stops": [(1, -60, TS + 60)]}], ts=TS)
    save(store, [{"id": "T2", "route": "1", "stops": [(1, 0, TS + 500)]}], ts=TS + 120)
    surround_with_polls(store, TS, skip=(600, 720))
    assert settled(store) == ["T1"]


def test_gap_report_covers_failures_and_silence(store):
    conn = store.connection()
    conn.execute(
        """INSERT INTO snapshots (feed, status, requested_at, feed_ts) VALUES
           ('trip_updates', 'ok', now() - interval '60 minutes', now() - interval '60 minutes'),
           ('trip_updates', 'timeout', now() - interval '55 minutes', NULL),
           ('trip_updates', 'ok', now() - interval '50 minutes', now() - interval '50 minutes'),
           ('trip_updates', 'ok', now() - interval '20 minutes', now() - interval '20 minutes')"""
    )
    rows = all_rows(
        store, (SQL / "gaps.sql").read_text(), {"trip_gap_s": 180, "position_gap_s": 900}
    )
    assert [(r[3], r[5]) for r in rows] == [
        (False, "timeout x1"),
        (False, "no attempts recorded (collector or database down)"),
        (True, "no attempts recorded (collector or database down)"),
    ]


def test_requests_count_excludes_budget_skips(store):
    store.record_attempt(Attempt("trip_updates", "timeout"))
    store.record_attempt(Attempt("trip_updates", "skipped_budget"))
    assert store.requests_last_24h() == 1
