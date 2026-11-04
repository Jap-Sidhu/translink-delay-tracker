# TransLink delay tracker

A data pipeline built to run continuously. It polls TransLink's real-time bus
feeds, stores them in Postgres, and reports how late buses run by route and hour.
This first version is the collector and its reports.

## Status and future plans

**Status:** the collector works and has been tested on real data. So far it has
run locally for a little over two hours, including a deliberate database outage.
It does not run continuously yet, so it has no findings about bus delays yet.

Planned next:

1. **Run it continuously.** Move the collector and database to an always-on server
   so collection no longer depends on a laptop staying awake.
2. **Build up history.** Collect weeks of data, covering weekdays, weekends and
   bad-weather days, and watch storage as it grows.
3. **Publish the findings.** Build a public page showing delays by route and hour,
   and cancellations, with the disclaimer TransLink's terms require.
4. **Predict delays.** Train a delay model and evaluate it against simple baselines
   on data it has not seen, reporting whether it actually beats them.

## Setup

You need Python 3.13, Docker, and a TransLink API key from
[translink.ca/developer](https://www.translink.ca/developer).

```bash
python3.13 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env        # then put your key in .env
docker compose up -d --wait
.venv/bin/python -m tracker.collector
```

The collector creates its tables on first connect. Stop it with Ctrl-C.

## Reports

```bash
.venv/bin/python -m tracker.report delay --min-trips 3   # by route and hour
.venv/bin/python -m tracker.report delay --route 099     # one route
.venv/bin/python -m tracker.report cancellations
.venv/bin/python -m tracker.report gaps
.venv/bin/python -m tracker.report summary
```

The delay query is also plain SQL in `sql/delay_by_route_hour.sql`. Positive
delay means late.

## Tests

```bash
docker compose exec db psql -U tracker -d translink -c "CREATE DATABASE translink_test"
TEST_DATABASE_URL=postgresql://tracker:tracker@localhost:5432/translink_test .venv/bin/python -m pytest
```

Without `TEST_DATABASE_URL`, the tests that need Postgres are skipped. No test
touches the network.

## Polling and TransLink's terms

[TransLink's terms](https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources/terms-of-use)
allow an API key 1,000 requests per day. That limit, not feed freshness, sets
the schedule:

| Feed | Every | Requests per day |
| --- | --- | --- |
| Trip updates | 120 s | 720 |
| Vehicle positions | 600 s | 144 |
| **Total** | | **864** |

Trip updates get most of the budget because they carry the delays. The other
136 requests leave room for restarts and manual testing.

- Every request is recorded in `snapshots`. Before each request the collector
  counts the last 24 hours and skips if it has reached `DAILY_REQUEST_BUDGET`
  (950). A rolling window is used because the terms do not say when a "day" starts.
- It refuses to start if the configured intervals could exceed the budget, or if
  the budget exceeds 1,000.
- Failed requests count against the budget, so nothing retries immediately.
  Failures back off exponentially to 30 minutes, and 401, 403 or 429 waits at
  least 15 minutes.
- Requests identify themselves as `translink-delay-tracker/0.1`.
- The static GTFS file is on a separate host with no API key. It is checked
  daily and only downloaded in full when it has changed.

Any page built on this data must show the disclaimer from clause 6 of the terms:

> Some of the data used in this product or service is provided by permission of
> TransLink. TransLink assumes no responsibility for the accuracy or currency of
> the Data used in this product or service.

## How delay is derived

The trip updates feed only describes the future. Each trip lists its upcoming
stops with a predicted arrival and a `delay` against schedule, and a stop leaves
the list once the bus has passed it. There is no record of what actually happened.

So the observed delay at a stop is **the last prediction published before the
stop left the feed**. `stop_delays` holds one row per trip, service day and stop,
overwritten on each poll. It stops changing when the stop drops out, and
`last_horizon_s` records how far ahead that final prediction was.

The view `observed_stop_delays` applies these rules:

| Rule | Why |
| --- | --- |
| The stop is absent from the newest snapshot | It has been passed, or its trip has ended |
| Trip is not canceled, stop is `SCHEDULED` | A canceled trip has no delay. Those trips stay in `trips` |
| `last_horizon_s` between -300 and 300 | A longer forecast is not an observation. This drops stops whose trip vanished, or that fell inside an outage |
| Delay between -1800 and +3600 s | See outliers below |

The check holds up against the schedule: in one live snapshot, `arrival.time - delay`
equalled the time in static `stop_times.txt` exactly for 23,195 of 23,242 stops.

Reports then read `settled_stop_delays`, which removes a further source of bias.
Near the newest snapshot, a stop only counts as passed if its bus ran early, and
near the first snapshot only late buses are left to see. An outage does both. An
early test showed the size of this: stops scheduled in the unfinished hour averaged
several minutes early on routes that were otherwise near zero. So any stop scheduled
within 30 minutes of the start of collection, the newest snapshot, or a gap over
300 s is left out. Figures therefore lag real time by 30 minutes, and each real
outage costs about an hour of data.

Hours are the **scheduled** local hour, in `America/Vancouver`, taken from
`arrival_time - delay`. Grouping by scheduled time stops a late bus moving into
the next hour.

### Known limits

- **This is a prediction, not a measurement.** With 120 s polling the last
  prediction is usually made within two minutes of arrival, but it can still be
  wrong by the size of that remaining forecast.
- **A trip's last stop has weaker evidence.** It leaves the feed with the whole
  trip, so a vanished trip and a finished one look alike. The horizon rule bounds
  the error.
- **Stops on a trip are not independent.** One late bus adds many late stops,
  which is why the report shows `n_trips` next to `n_stops`.
- **Prediction history is not kept.** Keeping every poll would add about 17 million
  rows a day. `stop_delays` keeps the first and last prediction per stop. Whether
  that is enough for the later model is an open question.

### Outliers

Raw values are always stored. `is_outlier` only keeps them out of the averages,
and `report summary` shows how many were excluded. In one live snapshot, 74 of
23,242 predictions were over 30 minutes from schedule, from just 10 trips, and
they ran through the whole trip. That points to a trip-level cause, such as a bus
assigned to the wrong trip, rather than noise at single stops. A bus that holds at
timepoints should not run 30 minutes early, so that is the lower cutoff. Late
buses are allowed up to an hour because severe lateness is real and is part of
what this project measures.

## Reliability

| Failure | What happens |
| --- | --- |
| Timeout, connection error, HTTP error | Recorded in `snapshots`, then backs off |
| Garbage or empty body | Recorded as `parse_error`, nothing written |
| Feed has not advanced | Recorded as `duplicate`, nothing written |
| Postgres down | No requests are spent, since nothing could be stored or budgeted. Reconnects every 30 s |
| Crash partway through a write | The snapshot and its rows share one transaction, so neither lands |
| Static GTFS unavailable | Logged and retried hourly. Delay collection continues |
| Route ID missing from static GTFS, or blank | Kept, and labelled `Unmatched route ...` |

`report gaps` lists every stretch without a successful snapshot and any failed
attempts inside it. It works from timestamps, so it also catches time the
collector was not running at all, like a sleeping laptop.

The API key travels as a URL parameter, so it can appear in `requests` error
messages. Errors are scrubbed before they are stored or logged, and the log
formatter scrubs every finished line, including tracebacks.

## Design notes

- `route_type` is stored from static GTFS and nothing filters on it. The feed
  was all buses when checked, but rail would flow through unchanged.
- A trip after midnight belongs to the previous service day, so `start_date`
  from the feed is part of each key.
- `direction_id`, `vehicle_id` and positions are collected for the prediction
  work, though no report uses them yet.
