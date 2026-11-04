"""All Postgres access for the collector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import psycopg

SCHEMA = (Path(__file__).parent / "schema.sql").read_text()

UPSERT_TRIP = """
INSERT INTO trips (start_date, trip_id, route_id, direction_id, vehicle_id,
                   schedule_relationship, ever_canceled, first_seen_at, last_seen_at)
VALUES (%(start_date)s, %(trip_id)s, %(route_id)s, %(direction_id)s, %(vehicle_id)s,
        %(schedule_relationship)s, %(ever_canceled)s, %(seen_at)s, %(seen_at)s)
ON CONFLICT (start_date, trip_id) DO UPDATE SET
    route_id = COALESCE(EXCLUDED.route_id, trips.route_id),
    direction_id = COALESCE(EXCLUDED.direction_id, trips.direction_id),
    vehicle_id = COALESCE(EXCLUDED.vehicle_id, trips.vehicle_id),
    schedule_relationship = EXCLUDED.schedule_relationship,
    ever_canceled = trips.ever_canceled OR EXCLUDED.ever_canceled,
    last_seen_at = EXCLUDED.last_seen_at
"""

UPSERT_STOP = """
INSERT INTO stop_delays (start_date, trip_id, stop_sequence, stop_id, schedule_relationship,
                         arrival_delay_s, arrival_time, last_horizon_s,
                         first_arrival_delay_s, first_seen_at, last_seen_at)
VALUES (%(start_date)s, %(trip_id)s, %(stop_sequence)s, %(stop_id)s, %(schedule_relationship)s,
        %(arrival_delay_s)s, %(arrival_time)s, %(last_horizon_s)s,
        %(arrival_delay_s)s, %(seen_at)s, %(seen_at)s)
ON CONFLICT (start_date, trip_id, stop_sequence) DO UPDATE SET
    stop_id = EXCLUDED.stop_id,
    schedule_relationship = EXCLUDED.schedule_relationship,
    arrival_delay_s = EXCLUDED.arrival_delay_s,
    arrival_time = EXCLUDED.arrival_time,
    last_horizon_s = EXCLUDED.last_horizon_s,
    last_seen_at = EXCLUDED.last_seen_at
"""

INSERT_POSITION = """
INSERT INTO vehicle_positions (vehicle_id, vehicle_ts, trip_id, start_date, route_id,
                               direction_id, latitude, longitude, current_stop_sequence,
                               stop_id, current_status)
VALUES (%(vehicle_id)s, %(vehicle_ts)s, %(trip_id)s, %(start_date)s, %(route_id)s,
        %(direction_id)s, %(latitude)s, %(longitude)s, %(current_stop_sequence)s,
        %(stop_id)s, %(current_status)s)
ON CONFLICT (vehicle_id, vehicle_ts) DO NOTHING
"""

UPSERT_ROUTE = """
INSERT INTO routes (route_id, short_name, long_name, route_type)
VALUES (%(route_id)s, %(short_name)s, %(long_name)s, %(route_type)s)
ON CONFLICT (route_id) DO UPDATE SET
    short_name = EXCLUDED.short_name,
    long_name = EXCLUDED.long_name,
    route_type = EXCLUDED.route_type,
    updated_at = now()
"""


@dataclass
class Attempt:
    """What happened on one request, whatever the outcome."""

    feed: str
    status: str
    http_status: int | None = None
    duration_ms: int | None = None
    detail: str | None = None


class Store:
    def __init__(self, database_url: str) -> None:
        self._url = database_url
        self._conn: psycopg.Connection | None = None

    def connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed or self._conn.broken:
            self._conn = psycopg.connect(self._url, autocommit=True, connect_timeout=5)
            self._conn.execute(SCHEMA)
        return self._conn

    def reset(self) -> None:
        """Drop the connection after a database error so the next call reconnects."""
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except psycopg.Error:
                pass

    def requests_last_24h(self) -> int:
        """Count real requests in a rolling window, which is stricter than any calendar day."""
        row = self.connection().execute(
            """SELECT count(*) FROM snapshots
               WHERE status <> 'skipped_budget' AND requested_at > now() - interval '24 hours'"""
        ).fetchone()
        return row[0]

    def record_attempt(self, attempt: Attempt) -> None:
        self.connection().execute(
            """INSERT INTO snapshots (feed, status, http_status, duration_ms, detail)
               VALUES (%s, %s, %s, %s, %s)""",
            (attempt.feed, attempt.status, attempt.http_status, attempt.duration_ms, attempt.detail),
        )

    def _save(self, cur, attempt, feed_ts, sha256, entity_count, write_rows) -> bool:
        """Record the snapshot and its rows in one transaction, or neither."""
        latest = cur.execute(
            """SELECT feed_ts, sha256 FROM snapshots
               WHERE feed = %s AND status = 'ok' ORDER BY feed_ts DESC LIMIT 1""",
            (attempt.feed,),
        ).fetchone()
        is_duplicate = latest is not None and (feed_ts <= latest[0] or sha256 == latest[1])
        cur.execute(
            """INSERT INTO snapshots (feed, status, feed_ts, sha256, entity_count,
                                      http_status, duration_ms, detail)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                attempt.feed,
                "duplicate" if is_duplicate else "ok",
                feed_ts,
                sha256,
                entity_count,
                attempt.http_status,
                attempt.duration_ms,
                "feed did not advance" if is_duplicate else attempt.detail,
            ),
        )
        if not is_duplicate:
            write_rows(cur)
        return not is_duplicate

    def save_trip_updates(self, attempt: Attempt, parsed, sha256: str) -> bool:
        conn = self.connection()
        with conn.transaction(), conn.cursor() as cur:
            def write(c) -> None:
                c.executemany(UPSERT_TRIP, parsed.trips)
                c.executemany(UPSERT_STOP, parsed.stops)

            return self._save(cur, attempt, parsed.feed_ts, sha256, parsed.entity_count, write)

    def save_positions(self, attempt: Attempt, parsed, sha256: str) -> bool:
        conn = self.connection()
        with conn.transaction(), conn.cursor() as cur:
            def write(c) -> None:
                c.executemany(INSERT_POSITION, parsed.positions)

            return self._save(cur, attempt, parsed.feed_ts, sha256, parsed.entity_count, write)

    def upsert_routes(self, routes: list[dict]) -> None:
        conn = self.connection()
        with conn.transaction(), conn.cursor() as cur:
            cur.executemany(UPSERT_ROUTE, routes)
