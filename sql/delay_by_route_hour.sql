-- Average delay by route and local scheduled hour.
-- Run with: psql "$DATABASE_URL" -f sql/delay_by_route_hour.sql
-- Stops on one trip are not independent, so read n_trips alongside n_stops.
SELECT
    route_label                                                         AS route,
    scheduled_hour                                                      AS hour,
    round(avg(arrival_delay_s))::int                                    AS avg_delay_s,
    round(percentile_cont(0.5) WITHIN GROUP (ORDER BY arrival_delay_s))::int AS median_delay_s,
    round(percentile_cont(0.9) WITHIN GROUP (ORDER BY arrival_delay_s))::int AS p90_delay_s,
    count(*)                                                            AS n_stops,
    count(DISTINCT (start_date, trip_id))                               AS n_trips
FROM settled_stop_delays
GROUP BY route_label, scheduled_hour
ORDER BY route_label, scheduled_hour;
