"""Resolve which dates a job run processes, from its processing type and arguments."""

import re
from datetime import UTC, datetime, timedelta


def validate_dates(date_ranges: list[str]) -> None:
    """Check that every date is a YYYY-MM-DD string before it becomes a partition path.

    Args:
        date_ranges: The dates to check.

    Raises:
        ValueError: If any date does not match YYYY-MM-DD, naming every one that fails.
    """
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    invalid_dates = [i for i in date_ranges if not date_pattern.match(i)]
    if invalid_dates:
        raise ValueError(f"Invalid dates: {invalid_dates}. Expected format: YYYY-MM-DD")


def generate_backfill_date_range(
    start_date: str | None,
    end_date: str | None,
) -> list[str]:
    """List every day from `start_date` to `end_date`, both included.

    A backfill must name both dates explicitly; neither ever defaults.

    Args:
        start_date: First day to process, as YYYY-MM-DD.
        end_date: Last day to process, as YYYY-MM-DD.

    Returns:
        Each day in the range as a YYYY-MM-DD string, in order.

    Raises:
        ValueError: If either date is missing, if `start_date` is after `end_date`, or
            if either date is later than today (UTC).
    """
    if start_date is None or end_date is None:
        raise ValueError("backfill requires both start_date and end_date.")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    if start > end:
        raise ValueError("start_date must be earlier than or the same as end_date.")

    if start.date() > datetime.now(UTC).date() or end.date() > datetime.now(UTC).date():
        raise ValueError(
            "start_date and end_date values must not take a value later than the current day."
        )

    date_range = [
        (start + timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range((end - start).days + 1)
    ]

    return date_range


def generate_lookback_date_range(
    lookback_days: int = 1,
    include_current_date: bool = False,
) -> list[str]:
    """List the days a lookback run processes, counted back from today (UTC).

    `lookback_days` days back, ending yesterday; `lookback_days=0` processes today.

    Args:
        lookback_days: How many days to look back.
        include_current_date: Extend the window through today.

    Returns:
        Each day in the window as a YYYY-MM-DD string, in order.

    Raises:
        ValueError: If `lookback_days` is negative.
    """
    if lookback_days < 0:
        raise ValueError("lookback_days must not be negative.")

    today = datetime.now(UTC).date()
    start = today - timedelta(days=lookback_days)
    end = today if (include_current_date or lookback_days == 0) else today - timedelta(days=1)

    date_range = [
        (start + timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range((end - start).days + 1)
    ]

    return date_range


def resolve_processing_dates(
    processing_type: str,
    lookback_days: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[str]:
    """Pick the date window for a run from its PROCESSING_TYPE argument.

    The one place a job turns its processing arguments into dates, so every job
    handles lookback and backfill the same way.

    Args:
        processing_type: `lookback` or `backfill`.
        lookback_days: Days to look back; required for `lookback`.
        start_date: First day, as YYYY-MM-DD; required for `backfill`.
        end_date: Last day, as YYYY-MM-DD; required for `backfill`.

    Returns:
        The days to process as YYYY-MM-DD strings, in order.

    Raises:
        ValueError: If `processing_type` is neither `lookback` nor `backfill`, or if the
            arguments that type needs are missing or invalid.
    """
    if processing_type == "lookback":
        if lookback_days is None:
            raise ValueError(
                "lookback PROCESSING_TYPE requires an integer value for lookback_days."
            )
        return generate_lookback_date_range(lookback_days=lookback_days)

    if processing_type == "backfill":
        return generate_backfill_date_range(
            start_date=start_date,
            end_date=end_date,
        )

    raise ValueError(f"PROCESSING_TYPE must be 'lookback' or 'backfill', not {processing_type!r}")
