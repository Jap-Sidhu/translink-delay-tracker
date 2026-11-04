-- Idempotent: applied every time the collector connects.

-- One row per request attempt, successful or not. This table is the record of
-- what happened, the source for the daily request budget, and the input to the
-- gap report.
CREATE TABLE IF NOT EXISTS snapshots (
    id            bigserial PRIMARY KEY,
    feed          text        NOT NULL CHECK (feed IN ('trip_updates', 'vehicle_positions')),
    requested_at  timestamptz NOT NULL DEFAULT now(),
    status        text        NOT NULL CHECK (status IN (
                      'ok', 'duplicate', 'timeout', 'connection_error',
                      'http_error', 'parse_error', 'skipped_budget')),
    feed_ts       timestamptz,
    sha256        text,
    entity_count  integer,
    http_status   integer,
    duration_ms   integer,
    detail        text
);
CREATE INDEX IF NOT EXISTS snapshots_feed_time ON snapshots (feed, requested_at);
-- A feed timestamp can only be ingested once, whatever the collector believes.
CREATE UNIQUE INDEX IF NOT EXISTS snapshots_one_ok_per_feed_ts
    ON snapshots (feed, feed_ts) WHERE status = 'ok';

-- Names from static GTFS. Rows are never deleted, so IDs from an older static
-- version keep their names after TransLink publishes a new one.
CREATE TABLE IF NOT EXISTS routes (
    route_id     text PRIMARY KEY,
    short_name   text,
    long_name    text,
    route_type   integer,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- One row per trip per service day, including canceled trips.
CREATE TABLE IF NOT EXISTS trips (
    start_date             date        NOT NULL,
    trip_id                text        NOT NULL,
    route_id               text,
    direction_id           smallint,
    vehicle_id             text,
    schedule_relationship  text        NOT NULL,
    ever_canceled          boolean     NOT NULL DEFAULT false,
    first_seen_at          timestamptz NOT NULL,
    last_seen_at           timestamptz NOT NULL,
    PRIMARY KEY (start_date, trip_id)
);
CREATE INDEX IF NOT EXISTS trips_canceled ON trips (start_date) WHERE ever_canceled;

-- One row per stop on a trip, updated on every poll while the stop is upcoming.
-- When the bus passes the stop leaves the feed, so the row freezes on the last
-- prediction made before the bus got there. See README for why.
CREATE TABLE IF NOT EXISTS stop_delays (
    start_date             date        NOT NULL,
    trip_id                text        NOT NULL,
    stop_sequence          integer     NOT NULL,
    stop_id                text,
    schedule_relationship  text        NOT NULL,
    arrival_delay_s        integer,
    arrival_time           timestamptz,
    last_horizon_s         integer,
    first_arrival_delay_s  integer,
    first_seen_at          timestamptz NOT NULL,
    last_seen_at           timestamptz NOT NULL,
    PRIMARY KEY (start_date, trip_id, stop_sequence),
    FOREIGN KEY (start_date, trip_id) REFERENCES trips (start_date, trip_id)
);
CREATE INDEX IF NOT EXISTS stop_delays_last_seen ON stop_delays (last_seen_at);

CREATE TABLE IF NOT EXISTS vehicle_positions (
    vehicle_id             text        NOT NULL,
    vehicle_ts             timestamptz NOT NULL,
    trip_id                text,
    start_date             date,
    route_id               text,
    direction_id           smallint,
    latitude               double precision,
    longitude              double precision,
    current_stop_sequence  integer,
    stop_id                text,
    current_status         text,
    PRIMARY KEY (vehicle_id, vehicle_ts)
);

-- Stops the bus has passed, one row each, with the reasons a row may not count.
-- "Passed" means the stop left the feed: its row was not refreshed by the newest
-- successful snapshot.
CREATE OR REPLACE VIEW stop_delay_candidates AS
SELECT
    s.start_date,
    s.trip_id,
    s.stop_sequence,
    s.stop_id,
    t.route_id,
    t.direction_id,
    t.vehicle_id,
    t.schedule_relationship AS trip_schedule_relationship,
    s.schedule_relationship AS stop_schedule_relationship,
    s.arrival_delay_s,
    s.arrival_time,
    s.arrival_time - make_interval(secs => s.arrival_delay_s) AS scheduled_arrival,
    s.last_horizon_s,
    s.last_seen_at,
    -- A trip that later vanished or a skipped poll leaves a stale forecast.
    -- 300 s tolerates one missed trip-update poll at the default 120 s interval.
    s.last_horizon_s BETWEEN -300 AND 300 AS is_recent_prediction,
    -- Early by over 30 minutes is not physically credible for a bus that holds
    -- at timepoints. Late by over an hour is treated the same way. See README.
    (s.arrival_delay_s < -1800 OR s.arrival_delay_s > 3600) AS is_outlier
FROM stop_delays s
JOIN trips t USING (start_date, trip_id)
WHERE s.last_seen_at < (
    SELECT max(feed_ts) FROM snapshots
    WHERE feed = 'trip_updates' AND status = 'ok'
);

-- The rows every delay figure is computed from.
CREATE OR REPLACE VIEW observed_stop_delays AS
SELECT
    c.*,
    (c.scheduled_arrival AT TIME ZONE 'America/Vancouver') AS scheduled_local,
    EXTRACT(HOUR FROM c.scheduled_arrival AT TIME ZONE 'America/Vancouver')::int AS scheduled_hour,
    r.short_name AS route_short_name,
    r.long_name  AS route_long_name,
    COALESCE(
        NULLIF(concat_ws(' ', r.short_name, r.long_name), ''),
        'Unmatched route ' || COALESCE(c.route_id, '(no id)')
    ) AS route_label
FROM stop_delay_candidates c
LEFT JOIN routes r USING (route_id)
WHERE c.trip_schedule_relationship <> 'CANCELED'
  AND c.stop_schedule_relationship = 'SCHEDULED'
  AND c.arrival_delay_s IS NOT NULL
  AND c.is_recent_prediction
  AND NOT c.is_outlier;

-- Times when the data under-represents one kind of bus. Near the newest snapshot
-- a stop only counts as passed if its bus ran early, and near the first snapshot
-- only late buses remain to be seen. An outage does both, and any gap longer than
-- the 300 s horizon rule loses stops outright.
CREATE OR REPLACE VIEW unreliable_periods AS
WITH ok AS (
    SELECT feed_ts,
           lag(feed_ts) OVER (ORDER BY feed_ts) AS previous_ts
    FROM snapshots
    WHERE feed = 'trip_updates' AND status = 'ok'
)
SELECT 'start of collection' AS reason, '-infinity'::timestamptz AS period_start,
       min(feed_ts) + interval '30 minutes' AS period_end
FROM ok HAVING count(*) > 0
UNION ALL
SELECT 'end of collection', max(feed_ts) - interval '30 minutes', 'infinity'::timestamptz
FROM ok HAVING count(*) > 0
UNION ALL
SELECT 'gap', previous_ts - interval '30 minutes', feed_ts + interval '30 minutes'
FROM ok
WHERE feed_ts - previous_ts > interval '300 seconds';

-- Observed delays with those periods removed. Reports read this view.
CREATE OR REPLACE VIEW settled_stop_delays AS
SELECT o.*
FROM observed_stop_delays o
WHERE NOT EXISTS (
    SELECT 1 FROM unreliable_periods u
    WHERE o.scheduled_arrival >= u.period_start
      AND o.scheduled_arrival <= u.period_end
);
