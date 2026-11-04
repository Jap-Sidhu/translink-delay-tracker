import pytest
from google.transit import gtfs_realtime_pb2 as pb

from conftest import TS, at, position_feed, trip_feed
from tracker.feed import FeedParseError, parse_positions, parse_trip_updates

FETCHED = at(TS + 5)


def test_parses_trip_and_stop_fields():
    payload = trip_feed([{"id": "T1", "route": "6612", "stops": [(24, -161, TS + 120)]}])
    parsed = parse_trip_updates(payload, FETCHED)
    assert parsed.feed_ts == at(TS)
    assert parsed.trips[0]["route_id"] == "6612"
    assert parsed.trips[0]["schedule_relationship"] == "SCHEDULED"
    stop = parsed.stops[0]
    assert (stop["stop_sequence"], stop["arrival_delay_s"], stop["last_horizon_s"]) == (24, -161, 120)
    assert stop["start_date"].isoformat() == "2026-11-03"


def test_canceled_trip_is_kept_without_stop_rows():
    payload = trip_feed([
        {"id": "C1", "route": "1", "relationship": pb.TripDescriptor.CANCELED, "stops": [(1, 0, TS)]},
    ])
    parsed = parse_trip_updates(payload, FETCHED)
    assert parsed.trips[0]["ever_canceled"] is True
    assert parsed.stops == []


def test_blank_route_id_keeps_the_trip():
    parsed = parse_trip_updates(trip_feed([{"id": "T1", "stops": [(1, 5, TS)]}]), FETCHED)
    assert parsed.trips[0]["route_id"] is None
    assert len(parsed.stops) == 1


def test_extreme_delays_are_kept_raw():
    payload = trip_feed([{"id": "T1", "route": "1", "stops": [(1, -4280, TS), (2, 3601, TS)]}])
    parsed = parse_trip_updates(payload, FETCHED)
    assert [s["arrival_delay_s"] for s in parsed.stops] == [-4280, 3601]


def test_stop_without_delay_is_stored_as_null():
    message = pb.FeedMessage()
    message.ParseFromString(trip_feed([{"id": "T1", "route": "1"}]))
    stop = message.entity[0].trip_update.stop_time_update.add()
    stop.stop_sequence = 3
    stop.schedule_relationship = pb.TripUpdate.StopTimeUpdate.SKIPPED
    parsed = parse_trip_updates(message.SerializeToString(), FETCHED)
    assert parsed.stops[0]["arrival_delay_s"] is None
    assert parsed.stops[0]["schedule_relationship"] == "SKIPPED"


def test_records_without_identity_are_counted_not_fatal():
    message = pb.FeedMessage()
    message.ParseFromString(trip_feed([{"id": "T1", "route": "1", "stops": [(1, 5, TS)]}]))
    message.entity.add(id="bad").trip_update.trip.route_id = "1"
    message.entity[0].trip_update.stop_time_update.add(stop_id="no-sequence")
    parsed = parse_trip_updates(message.SerializeToString(), FETCHED)
    assert parsed.skipped == 2
    assert len(parsed.trips) == 1


def test_missing_start_date_uses_local_service_date():
    # 02:00 UTC on 4 Nov is still 3 Nov in Vancouver.
    fetched = at(TS)
    payload = trip_feed([{"id": "T1", "route": "1", "start_date": ""}], ts=1_793_757_600)
    parsed = parse_trip_updates(payload, fetched)
    assert parsed.trips[0]["start_date"].isoformat() == "2026-11-03"


def test_missing_header_timestamp_falls_back_to_fetch_time():
    payload = trip_feed([], ts=0)
    assert parse_trip_updates(payload, FETCHED).feed_ts == FETCHED


@pytest.mark.parametrize(
    "payload",
    [b"", b"<html>503 Service Unavailable</html>", b"\xff\xff\xff\xff", trip_feed([])[:5]],
)
def test_bad_bodies_raise_parse_error(payload):
    with pytest.raises(FeedParseError):
        parse_trip_updates(payload, FETCHED)


def test_parses_vehicle_positions_without_assuming_mode():
    parsed = parse_positions(position_feed([("18349", TS - 30)]), FETCHED)
    row = parsed.positions[0]
    assert row["vehicle_id"] == "18349"
    assert row["vehicle_ts"] == at(TS - 30)
    assert row["trip_id"] is None
    assert row["latitude"] == pytest.approx(49.28)
