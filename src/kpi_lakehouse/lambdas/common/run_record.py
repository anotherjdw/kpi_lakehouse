"""The run record a Lambda function in this project prints: one line of JSON per run.

The Lambda counterpart of the Glue jobs' record in `runtime/job_settings.py`: the same
`RUN_RECORD` prefix and the same names for the fields both have, plus Lambda's request id,
and none of the Glue-only fields. It is a separate class because the Lambda package doesn't
ship the Glue code.

The function can't publish an alert itself: it runs in a VPC with no route to SNS. Lambda
sends its stdout to the function's log group without the VPC, where metric filters count the
`FAILED` and `SUCCEEDED` records for the function's alarms.
"""

from __future__ import annotations

import json
import time
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

RUN_RECORD_PREFIX = "RUN_RECORD"

# The fields printed first, as in the Glue jobs' record, so a metric filter always finds
# them at the start of the line.
RECORD_FIRST_KEYS = ("status", "job_name", "request_id", "phase")


@dataclass
class LambdaRunRecord:
    """One structured summary of a single Lambda invocation.

    Mutable by design: `lambda_run` fills in the outcome fields, `phase` tracks which step
    is running, and the function may attach anything else to `metrics`, such as the rows it
    wrote per dataset.
    """

    job_name: str
    started_at: str
    request_id: str | None = None
    status: str = "RUNNING"
    phase: str | None = None
    finished_at: str | None = None
    duration_seconds: float | None = None
    phase_durations: dict[str, float] = field(default_factory=dict)
    error_type: str | None = None
    error_message: str | None = None
    traceback: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)


@contextmanager
def phase(record: LambdaRunRecord, name: str) -> Iterator[None]:
    """Mark which step of the function is running, and time it.

    Nothing resets `record.phase` on the way out of a failure, so it still names the step
    that was running when the exception escaped; `lambda_run` clears it on success.

    Args:
        record: The run's record.
        name: The step's name.
    """
    record.phase = name
    started = time.monotonic()
    try:
        yield
    finally:
        record.phase_durations[name] = round(time.monotonic() - started, 3)


@contextmanager
def lambda_run(context: object, local_name: str) -> Iterator[LambdaRunRecord]:
    """Wrap one invocation so that exactly one run record is printed when it ends.

    On failure the record is completed with the error and printed, then the exception is
    re-raised unchanged, so Lambda still reports the invocation as failed. A run that hits
    the function's timeout prints no record: Lambda ends it without running any more of
    its code, and logs its own `Task timed out` line instead.

    Args:
        context: Lambda's context object, for the function's name and the request id; None
            in local runs and tests.
        local_name: The name recorded when there is no context.

    Yields:
        The mutable `LambdaRunRecord` for this run.
    """
    record = LambdaRunRecord(
        job_name=getattr(context, "function_name", None) or local_name,
        started_at=_utc_now_iso(),
        request_id=getattr(context, "aws_request_id", None),
    )
    started = time.monotonic()
    try:
        yield record
    except BaseException as exc:
        record.status = "FAILED"
        record.error_type = type(exc).__name__
        record.error_message = str(exc)[:2000]
        record.traceback = traceback.format_exc()[-4000:]
        raise
    else:
        record.status = "SUCCEEDED"
        record.phase = None
    finally:
        record.finished_at = _utc_now_iso()
        record.duration_seconds = round(time.monotonic() - started, 3)
        emit_run_record(record)


def emit_run_record(record: LambdaRunRecord) -> None:
    """Print the run record to stdout as a single line of JSON, key fields first.

    Lambda writes each line of stdout to the function's log group as one log event, so a
    metric filter sees the whole record at once.

    Args:
        record: The completed run record.
    """
    fields = asdict(record)
    ordered = {key: fields.pop(key) for key in RECORD_FIRST_KEYS}
    ordered.update(sorted(fields.items()))
    payload = json.dumps(ordered, default=str)
    print(f"{RUN_RECORD_PREFIX} {payload}", flush=True)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")
