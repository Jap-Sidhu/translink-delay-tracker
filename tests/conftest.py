from __future__ import annotations

from datetime import datetime, timezone

from google.transit import gtfs_realtime_pb2 as pb
import pytest

TS = 1_793_749_076


def at(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def trip_feed(trips: list[dict], ts: int = TS) -> bytes:
    """Build a real serialized feed. Each trip dict: id, route, relationship, stops."""
    message = pb.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.timestamp = ts
    for trip in trips:
        entity = message.entity.add()
        entity.id = trip["id"]
        update = entity.trip_update
        update.trip.trip_id = trip["id"]
        if trip.get("route"):
            update.trip.route_id = trip["route"]
        if trip.get("start_date", "20261103"):
            update.trip.start_date = trip.get("start_date", "20261103")
        update.trip.direction_id = 0
        update.trip.schedule_relationship = trip.get("relationship", 0)
        update.vehicle.id = "V" + trip["id"]
        for sequence, delay, eta in trip.get("stops", []):
            stop = update.stop_time_update.add()
            stop.stop_sequence = sequence
            stop.stop_id = f"S{sequence}"
            stop.arrival.delay = delay
            stop.arrival.time = eta
    return message.SerializeToString()


def position_feed(vehicles: list[tuple[str, int]], ts: int = TS) -> bytes:
    message = pb.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.timestamp = ts
    for vehicle_id, vehicle_ts in vehicles:
        entity = message.entity.add()
        entity.id = vehicle_id
        entity.vehicle.vehicle.id = vehicle_id
        entity.vehicle.timestamp = vehicle_ts
        entity.vehicle.position.latitude = 49.28
        entity.vehicle.position.longitude = -123.14
    return message.SerializeToString()


@pytest.fixture
def builders():
    return trip_feed, position_feed
