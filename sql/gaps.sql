-- Stretches with no successful snapshot, including one still open now. This
-- works from timestamps rather than from error events, so it also catches time
-- the collector was not running at all, such as a sleeping laptop.
-- Parameters: trip_gap_s, position_gap_s.
WITH ok AS (
    SELECT feed, requested_at FROM snapshots WHERE status = 'ok'
), spans AS (
    SELECT feed,
           lag(requested_at) OVER (PARTITION BY feed ORDER BY requested_at) AS gap_start,
           requested_at AS gap_end,
           false AS still_open
    FROM ok
    UNION ALL
    SELECT feed, max(requested_at), now(), true FROM ok GROUP BY feed
)
SELECT
    s.feed,
    s.gap_start,
    s.gap_end,
    s.still_open,
    round(EXTRACT(EPOCH FROM s.gap_end - s.gap_start))::int AS gap_s,
    COALESCE((
        SELECT string_agg(x.status || ' x' || x.n, ', ' ORDER BY x.status)
        FROM (
            SELECT f.status, count(*) AS n FROM snapshots f
            WHERE f.feed = s.feed AND f.status <> 'ok'
              AND f.requested_at > s.gap_start AND f.requested_at < s.gap_end
            GROUP BY f.status
        ) x
    ), 'no attempts recorded (collector or database down)') AS attempts
FROM spans s
WHERE s.gap_start IS NOT NULL
  AND s.gap_end - s.gap_start > make_interval(secs =>
      CASE s.feed WHEN 'trip_updates' THEN %(trip_gap_s)s ELSE %(position_gap_s)s END)
ORDER BY s.gap_start;
