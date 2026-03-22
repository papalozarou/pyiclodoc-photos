# ------------------------------------------------------------------------------
# This module provides timezone-aware current time helpers based on the
# container "TZ" setting.
# ------------------------------------------------------------------------------

from __future__ import annotations

from datetime import datetime, timezone, tzinfo
import os

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


# ------------------------------------------------------------------------------
# This function returns timezone from "TZ", falling back to UTC.
#
# Returns: A valid timezone object resolved from IANA zone data.
#
# N.B.
# Invalid zone names do not fail worker startup. The runtime falls back to UTC
# so scheduling and log timestamps stay deterministic.
#
# Notes: ZoneInfo behaviour follows Python standard library guidance:
# https://docs.python.org/3/library/zoneinfo.html
# ------------------------------------------------------------------------------
def configured_timezone() -> tzinfo:
    TZ_NAME = os.getenv("TZ", "UTC").strip() or "UTC"

    try:
        return ZoneInfo(TZ_NAME)
    except ZoneInfoNotFoundError:
        return timezone.utc


# ------------------------------------------------------------------------------
# This function reports whether "TZ" falls back to UTC because it is invalid.
#
# Returns: Warning text when fallback is in effect, otherwise an empty string.
# ------------------------------------------------------------------------------
def get_timezone_fallback_warning() -> str:
    TZ_NAME = os.getenv("TZ", "UTC").strip() or "UTC"

    try:
        ZoneInfo(TZ_NAME)
    except ZoneInfoNotFoundError:
        return (
            f'TZ="{TZ_NAME}" is invalid. Falling back to UTC for schedule '
            "calculations and timestamps."
        )

    return ""


# ------------------------------------------------------------------------------
# This function returns the current time in the configured timezone.
#
# Returns: Offset-aware "datetime" in the configured timezone.
# ------------------------------------------------------------------------------
def now_local() -> datetime:
    return datetime.now(configured_timezone())


# ------------------------------------------------------------------------------
# This function returns a configured-timezone ISO-8601 timestamp.
#
# Returns: Offset-aware ISO-8601 timestamp string.
# ------------------------------------------------------------------------------
def now_local_iso() -> str:
    return now_local().isoformat()
