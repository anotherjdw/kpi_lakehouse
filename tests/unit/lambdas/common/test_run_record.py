"""The run record a Lambda function prints: one line per invocation, key fields first."""

import json
from types import SimpleNamespace
from typing import Any

import pytest

from kpi_lakehouse.lambdas.common.run_record import RUN_RECORD_PREFIX, lambda_run, phase

CONTEXT = SimpleNamespace(function_name="shop-sim-extract", aws_request_id="req-123")


def printed_records(out: str) -> list[dict[str, Any]]:
    """The run records printed to stdout."""
    prefix = f"{RUN_RECORD_PREFIX} "
    return [json.loads(line[len(prefix) :]) for line in out.splitlines() if line.startswith(prefix)]


def test_success_prints_one_succeeded_record(capsys: pytest.CaptureFixture[str]) -> None:
    """A run that succeeds prints one SUCCEEDED record, with its metrics and step timings."""
    with lambda_run(CONTEXT, local_name="local") as record, phase(record, "write"):
        record.metrics["rows"] = {"orders": 3}

    [printed] = printed_records(capsys.readouterr().out)
    assert printed["status"] == "SUCCEEDED"
    assert printed["job_name"] == "shop-sim-extract"
    assert printed["request_id"] == "req-123"
    assert printed["phase"] is None
    assert printed["metrics"] == {"rows": {"orders": 3}}
    assert set(printed["phase_durations"]) == {"write"}


def test_failure_prints_failed_record_and_reraises(capsys: pytest.CaptureFixture[str]) -> None:
    """A run that fails prints one FAILED record naming the step and error, then re-raises."""
    with (
        pytest.raises(OSError, match="S3 unavailable"),
        lambda_run(CONTEXT, local_name="local") as record,
        phase(record, "write"),
    ):
        raise OSError("S3 unavailable")

    [printed] = printed_records(capsys.readouterr().out)
    assert printed["status"] == "FAILED"
    assert printed["phase"] == "write"
    assert printed["error_type"] == "OSError"
    assert printed["error_message"] == "S3 unavailable"
    assert "Traceback" in printed["traceback"]


def test_without_context_records_the_local_name(capsys: pytest.CaptureFixture[str]) -> None:
    """Run locally, with no Lambda context, the record names the function and has no request id."""
    with lambda_run(None, local_name="shop-sim-extract"):
        pass

    [printed] = printed_records(capsys.readouterr().out)
    assert printed["job_name"] == "shop-sim-extract"
    assert printed["request_id"] is None


def test_record_is_one_line_with_key_fields_first(capsys: pytest.CaptureFixture[str]) -> None:
    """The record is one line, a traceback included, and starts with status and job name."""
    with pytest.raises(ValueError), lambda_run(CONTEXT, local_name="local"):
        raise ValueError("line one\nline two")

    out = capsys.readouterr().out
    assert len(out.splitlines()) == 1
    assert out.startswith(
        f'{RUN_RECORD_PREFIX} {{"status": "FAILED", "job_name": "shop-sim-extract", '
        '"request_id": "req-123", "phase": null,'
    )
