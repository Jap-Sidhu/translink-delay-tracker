-- Trips TransLink marked canceled, by route. Reinstated trips are counted too;
-- still_canceled shows how many stayed canceled at last sight.
SELECT
    COALESCE(NULLIF(concat_ws(' ', r.short_name, r.long_name), ''),
             'Unmatched route ' || COALESCE(t.route_id, '(no id)')) AS route,
    count(*) FILTER (WHERE t.ever_canceled)                          AS ever_canceled,
    count(*) FILTER (WHERE t.schedule_relationship = 'CANCELED')     AS still_canceled,
    count(*)                                                         AS trips_seen,
    round(100.0 * count(*) FILTER (WHERE t.ever_canceled) / count(*), 1) AS pct_canceled
FROM trips t
LEFT JOIN routes r USING (route_id)
GROUP BY 1
HAVING count(*) FILTER (WHERE t.ever_canceled) > 0
ORDER BY ever_canceled DESC, route;
