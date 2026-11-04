"""Download and parse GTFS-realtime feeds. No database code lives here."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import requests
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2 as pb

from .config import USER_AGENT

LOCAL_TZ = ZoneInfo("America/Vancouver")
CONNECT_TIMEOUT_S = 10
READ_TIMEOUT_S = 30
_APIKEY_PARAM = re.compile(r"(apikey=)[^&\s'\"]+", re.IGNORECASE)


def scrub(text: str, api_key: str = "") -> str:
    """Remove the API key from text. It travels in the URL, so errors can carry it."""
    if api_key:
        text = text.replace(api_key, "[REDACTED]")
    return _APIKEY_PARAM.sub(r"\1[REDACTED]", text)


class RedactingFormatter(logging.Formatter):
    """Scrub finished log lines, including any traceback text."""

    def __init__(self, fmt: str, api_key: str = "") -> None:
        super().__init__(fmt)
        self._api_key = api_key

    def format(self, record: logging.LogRecord) -> str:
        return scrub(super().format(record), self._api_key)


class FetchError(Exception):
    """A request failed. The message is always scrubbed of the API key."""

    def __init__(self, status: str, message: str, http_status: int | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.http_status = http_status


class FeedParseError(Exception):
    """The body was not a usable GTFS-realtime message."""


def fetch_feed(session: requests.Session, url: str, api_key: str) -> bytes:
    """Fetch one feed once. Retrying is the caller's choice, since every try spends budget."""
    try:
        response = session.get(
            url,
            params={"apikey": api_key},
            headers={"User-Agent": USER_AGENT},
            timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
        )
    # `from None` drops the original exception, whose text contains the full URL.
    except requests.Timeout as exc:
        raise FetchError("timeout", scrub(str(exc), api_key)) from None
    except requests.RequestException as exc:
        raise FetchError("connection_error", scrub(str(exc), api_key)) from None
    if response.status_code != 200:
        raise FetchError(
            "http_error",
            f"HTTP {response.status_code} from {url}",
            http_status=response.status_code,
        )
    return response.content


def _utc(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _enum_name(enum_type, value: int) -> str:
    try:
        return enum_type.Name(value)
    except ValueError:
        return f"UNKNOWN_{value}"


def _service_date(raw: str, feed_ts: datetime) -> date:
    """Use the trip's start_date, since a trip after midnight belongs to the previous day."""
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError:
        return feed_ts.astimezone(LOCAL_TZ).date()


def _load(payload: bytes) -> pb.FeedMessage:
    if not payload:
        raise FeedParseError("empty response body")
    message = pb.FeedMessage()
    try:
        message.ParseFromString(payload)
    except DecodeError as exc:
        raise FeedParseError(f"not a GTFS-realtime message: {exc}") from None
    if not message.header.gtfs_realtime_version:
        raise FeedParseError("message has no GTFS-realtime header")
    return message


def _feed_ts(message: pb.FeedMessage, fetched_at: datetime) -> datetime:
    return _utc(message.header.timestamp) if message.header.timestamp else fetched_at


@dataclass
class ParsedTripUpdates:
    feed_ts: datetime
    entity_count: int
    trips: list[dict] = field(default_factory=list)
    stops: list[dict] = field(default_factory=list)
    skipped: int = 0


@dataclass
class ParsedPositions:
    feed_ts: datetime
    entity_count: int
    positions: list[dict] = field(default_factory=list)
    skipped: int = 0


def parse_trip_updates(payload: bytes, fetched_at: datetime) -> ParsedTripUpdates:
    message = _load(payload)
    feed_ts = _feed_ts(message, fetched_at)
    parsed = ParsedTripUpdates(feed_ts=feed_ts, entity_count=len(message.entity))
    canceled = pb.TripDescriptor.ScheduleRelationship.CANCELED
    relationship = pb.TripUpdate.StopTimeUpdate.ScheduleRelationship

    for entity in message.entity:
        if not entity.HasField("trip_update"):
            continue
        update = entity.trip_update
        trip = update.trip
        if not trip.trip_id:
            parsed.skipped += 1
            continue
        start_date = _service_date(trip.start_date, feed_ts)
        parsed.trips.append(
            {
                "start_date": start_date,
                "trip_id": trip.trip_id,
                # Blank route IDs occur in the live feed. Keep the trip.
                "route_id": trip.route_id or None,
                "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
                "vehicle_id": update.vehicle.id or None,
                "schedule_relationship": _enum_name(
                    pb.TripDescriptor.ScheduleRelationship, trip.schedule_relationship
                ),
                "ever_canceled": trip.schedule_relationship == canceled,
                "seen_at": feed_ts,
            }
        )
        # A canceled trip has no delay to report: every stop is SKIPPED.
        if trip.schedule_relationship == canceled:
            continue
        for stop in update.stop_time_update:
            if not stop.HasField("stop_sequence"):
                parsed.skipped += 1
                continue
            arrival_time = _utc(stop.arrival.time) if stop.arrival.HasField("time") else None
            parsed.stops.append(
                {
                    "start_date": start_date,
                    "trip_id": trip.trip_id,
                    "stop_sequence": stop.stop_sequence,
                    "stop_id": stop.stop_id or None,
                    "schedule_relationship": _enum_name(relationship, stop.schedule_relationship),
                    "arrival_delay_s": stop.arrival.delay if stop.arrival.HasField("delay") else None,
                    "arrival_time": arrival_time,
                    "last_horizon_s": (
                        round((arrival_time - feed_ts).total_seconds()) if arrival_time else None
                    ),
                    "seen_at": feed_ts,
                }
            )
    return parsed


def parse_positions(payload: bytes, fetched_at: datetime) -> ParsedPositions:
    message = _load(payload)
    feed_ts = _feed_ts(message, fetched_at)
    parsed = ParsedPositions(feed_ts=feed_ts, entity_count=len(message.entity))
    for entity in message.entity:
        if not entity.HasField("vehicle"):
            continue
        vehicle = entity.vehicle
        if not vehicle.vehicle.id or not vehicle.timestamp:
            parsed.skipped += 1
            continue
        trip = vehicle.trip
        parsed.positions.append(
            {
                "vehicle_id": vehicle.vehicle.id,
                "vehicle_ts": _utc(vehicle.timestamp),
                "trip_id": trip.trip_id or None,
                "start_date": _service_date(trip.start_date, feed_ts) if trip.trip_id else None,
                "route_id": trip.route_id or None,
                "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
                "latitude": vehicle.position.latitude if vehicle.HasField("position") else None,
                "longitude": vehicle.position.longitude if vehicle.HasField("position") else None,
                "current_stop_sequence": (
                    vehicle.current_stop_sequence
                    if vehicle.HasField("current_stop_sequence")
                    else None
                ),
                "stop_id": vehicle.stop_id or None,
                "current_status": _enum_name(
                    pb.VehiclePosition.VehicleStopStatus, vehicle.current_status
                ),
            }
        )
    return parsed
