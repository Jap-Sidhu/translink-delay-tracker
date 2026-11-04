"""Print findings from Postgres.

    python -m tracker.report delay [--route 99] [--min-trips 3]
    python -m tracker.report gaps
    python -m tracker.report cancellations
    python -m tracker.report summary
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from .config import DEFAULT_DATABASE_URL

SQL_DIR = Path(__file__).parent.parent / "sql"

SUMMARY = """
SELECT 'snapshots ' || feed || ' / ' || status AS measure, count(*)::text AS value
FROM snapshots GROUP BY feed, status
UNION ALL SELECT 'first requested_at', min(requested_at)::text FROM snapshots
UNION ALL SELECT 'last requested_at', max(requested_at)::text FROM snapshots
UNION ALL SELECT 'requests in last 24h', count(*)::text FROM snapshots
          WHERE status <> 'skipped_budget' AND requested_at > now() - interval '24 hours'
UNION ALL SELECT 'trips seen', count(*)::text FROM trips
UNION ALL SELECT 'trips ever canceled', count(*)::text FROM trips WHERE ever_canceled
UNION ALL SELECT 'stop rows stored', count(*)::text FROM stop_delays
UNION ALL SELECT 'passed stops', count(*)::text FROM stop_delay_candidates
UNION ALL SELECT 'excluded: canceled trip', count(*)::text FROM stop_delay_candidates
          WHERE trip_schedule_relationship = 'CANCELED'
UNION ALL SELECT 'excluded: stop not scheduled', count(*)::text FROM stop_delay_candidates
          WHERE stop_schedule_relationship <> 'SCHEDULED'
UNION ALL SELECT 'excluded: no recent prediction', count(*)::text FROM stop_delay_candidates
          WHERE NOT COALESCE(is_recent_prediction, false)
UNION ALL SELECT 'excluded: outlier', count(*)::text FROM stop_delay_candidates WHERE is_outlier
UNION ALL SELECT 'observed stop delays', count(*)::text FROM observed_stop_delays
UNION ALL SELECT 'settled stop delays (used in reports)', count(*)::text FROM settled_stop_delays
UNION ALL SELECT 'observed, route not in static GTFS', count(*)::text
          FROM observed_stop_delays WHERE route_short_name IS NULL AND route_long_name IS NULL
UNION ALL SELECT 'vehicle position rows', count(*)::text FROM vehicle_positions
UNION ALL SELECT 'routes loaded', count(*)::text FROM routes
UNION ALL SELECT 'database size', pg_size_pretty(pg_database_size(current_database()))
"""


def print_table(columns: list[str], rows: list[tuple]) -> None:
    cells = [["" if value is None else str(value) for value in row] for row in rows]
    widths = [max([len(c)] + [len(row[i]) for row in cells]) for i, c in enumerate(columns)]
    line = "  ".join(c.ljust(w) for c, w in zip(columns, widths))
    print(line)
    print("-" * len(line))
    for row in cells:
        print("  ".join(v.ljust(w) for v, w in zip(row, widths)))
    print(f"({len(rows)} rows)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report on collected TransLink data.")
    parser.add_argument("report", choices=["delay", "gaps", "cancellations", "summary"])
    parser.add_argument("--route", help="only routes whose name contains this text")
    parser.add_argument("--min-trips", type=int, default=1, help="hide thin route-hour cells")
    args = parser.parse_args(argv)

    load_dotenv()
    url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    trip_poll = int(os.environ.get("TRIP_POLL_SECONDS", 120))
    position_poll = int(os.environ.get("POSITION_POLL_SECONDS", 600))

    with psycopg.connect(url, autocommit=True) as conn:
        if args.report == "summary":
            cursor = conn.execute(SUMMARY)
        elif args.report == "gaps":
            cursor = conn.execute(
                (SQL_DIR / "gaps.sql").read_text(),
                {"trip_gap_s": trip_poll * 1.5, "position_gap_s": position_poll * 1.5},
            )
        elif args.report == "delay":
            cursor = conn.execute((SQL_DIR / "delay_by_route_hour.sql").read_text())
        else:
            cursor = conn.execute((SQL_DIR / "cancellations.sql").read_text())
        columns = [c.name for c in cursor.description]
        rows = cursor.fetchall()

    if args.report == "delay":
        rows = [r for r in rows if r[columns.index("n_trips")] >= args.min_trips]
        if args.route:
            rows = [r for r in rows if args.route.lower() in r[0].lower()]
    print_table(columns, rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
