"""Settings from the environment, with .env loaded when present."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# TransLink's Open API terms allow an API key 1,000 requests per day.
TERMS_DAILY_REQUEST_LIMIT = 1000
USER_AGENT = "translink-delay-tracker/0.1"
TRIP_UPDATES_URL = "https://gtfsapi.translink.ca/v3/gtfsrealtime"
VEHICLE_POSITIONS_URL = "https://gtfsapi.translink.ca/v3/gtfsposition"
STATIC_GTFS_URL = "https://gtfs-static.translink.ca/gtfs/google_transit.zip"
DEFAULT_DATABASE_URL = "postgresql://tracker:tracker@localhost:5432/translink"
SECONDS_PER_DAY = 86_400


class ConfigError(Exception):
    """The settings cannot produce a working, terms-compliant collector."""


@dataclass(frozen=True)
class Config:
    api_key: str
    database_url: str = DEFAULT_DATABASE_URL
    trip_poll_seconds: int = 120
    position_poll_seconds: int = 600
    daily_request_budget: int = 950

    def requests_per_day(self) -> float:
        return (
            SECONDS_PER_DAY / self.trip_poll_seconds
            + SECONDS_PER_DAY / self.position_poll_seconds
        )

    def validate(self) -> None:
        if not self.api_key:
            raise ConfigError("TRANSLINK_API_KEY is not set. Copy .env.example to .env.")
        if self.daily_request_budget > TERMS_DAILY_REQUEST_LIMIT:
            raise ConfigError(
                f"DAILY_REQUEST_BUDGET must not exceed {TERMS_DAILY_REQUEST_LIMIT}, "
                "TransLink's daily limit."
            )
        if self.requests_per_day() > self.daily_request_budget:
            raise ConfigError(
                f"Poll intervals need {self.requests_per_day():.0f} requests per day, "
                f"over the budget of {self.daily_request_budget}. Poll less often."
            )


def load_config() -> Config:
    load_dotenv()
    config = Config(
        api_key=os.environ.get("TRANSLINK_API_KEY", "").strip(),
        database_url=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
        trip_poll_seconds=int(os.environ.get("TRIP_POLL_SECONDS", 120)),
        position_poll_seconds=int(os.environ.get("POSITION_POLL_SECONDS", 600)),
        daily_request_budget=int(os.environ.get("DAILY_REQUEST_BUDGET", 950)),
    )
    config.validate()
    return config
