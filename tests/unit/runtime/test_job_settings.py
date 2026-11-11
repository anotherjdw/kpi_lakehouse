"""The run record, the alert a failed or stopped run publishes, and the stop signal.

The SNS client is replaced by a fake that records what it is asked to publish, so these
tests need no AWS account and no Spark session.
"""

import json
import os
import signal
import sys
from argparse import Namespace
from typing import Any

import pytest

from kpi_lakehouse.runtime import job_settings
from kpi_lakehouse.runtime.job_settings import (
    RUN_RECORD_PREFIX,
    JobStopped,
    RunRecord,
    alert_message,
    alert_subject,
    alert_topic_arn,
    job_run,
    phase,
)

TOPIC_ARN = "arn:aws:sns:eu-central-1:123456789012:kpi_lakehouse-prod-alerts"
ARGS = Namespace(
    JOB_NAME="kpi_lakehouse-prod-customers", JOB_RUN_ID="jr_abc123", environment="prod"
)
DATES = {"customers": ["2026-11-09", "2026-11-10"]}


class FakeSns:
    """Records each publish instead of calling SNS; raises `error` instead, when set."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.regions: list[str] = []
        self.published: list[dict[str, str]] = []

    def client(self, region: str) -> "FakeSns":
        self.regions.append(region)
        return self

    def publish(self, *, TopicArn: str, Subject: str, Message: str) -> Any:
        if self.error is not None:
            raise self.error
        self.published.append({"TopicArn": TopicArn, "Subject": Subject, "Message": Message})
        return {"MessageId": "1"}


@pytest.fixture
def sns(monkeypatch: pytest.MonkeyPatch) -> FakeSns:
    """A fake SNS client in place of boto3's."""
    fake = FakeSns()
    monkeypatch.setattr(job_settings, "sns_client", fake.client)
    return fake


@pytest.fixture
def with_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    """The process's arguments as Glue passes them, with the alert topic."""
    monkeypatch.setattr(sys, "argv", ["customers.py", "--ALERT_TOPIC_ARN", TOPIC_ARN])


@pytest.fixture
def without_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    """The process's arguments in a local run, with no alert topic."""
    monkeypatch.setattr(sys, "argv", ["customers.py"])


def emitted_records(out: str) -> list[dict[str, Any]]:
    """The run records printed to stdout."""
    prefix = f"{RUN_RECORD_PREFIX} "
    return [json.loads(line[len(prefix) :]) for line in out.splitlines() if line.startswith(prefix)]


def test_success_emits_succeeded_and_publishes_nothing(
    sns: FakeSns, with_topic: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """A run that succeeds emits one SUCCEEDED record and sends no alert."""
    with job_run("customers", ARGS, DATES) as record, phase(record, "read"):
        pass

    [emitted] = emitted_records(capsys.readouterr().out)
    assert emitted["status"] == "SUCCEEDED"
    assert emitted["phase"] is None
    assert sns.published == []


def test_failure_publishes_one_alert_and_reraises(
    sns: FakeSns, with_topic: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """A failed run emits a FAILED record, publishes one alert to the topic, and re-raises."""
    with (
        pytest.raises(ValueError, match="read no rows"),
        job_run("customers", ARGS, DATES) as record,
        phase(record, "read"),
    ):
        raise ValueError("Source 'customers' read no rows")

    [emitted] = emitted_records(capsys.readouterr().out)
    assert emitted["status"] == "FAILED"
    assert emitted["phase"] == "read"
    assert sns.regions == ["eu-central-1"]
    [alert] = sns.published
    assert alert["TopicArn"] == TOPIC_ARN
    assert alert["Subject"] == "[kpi_lakehouse prod] customers FAILED in read"
    assert "Error: ValueError: Source 'customers' read no rows" in alert["Message"]


def test_failure_without_topic_publishes_nothing(
    sns: FakeSns, without_topic: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """Without --ALERT_TOPIC_ARN, as in a local run, a failure is recorded but not published."""
    with pytest.raises(ValueError), job_run("customers", ARGS, DATES):
        raise ValueError("boom")

    [emitted] = emitted_records(capsys.readouterr().out)
    assert emitted["status"] == "FAILED"
    assert sns.regions == []


def test_failed_publish_never_hides_the_job_error(
    monkeypatch: pytest.MonkeyPatch, with_topic: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """A publish that raises is printed on its own line, and the job's own error still escapes."""
    fake = FakeSns(error=ConnectionError("endpoint unreachable"))
    monkeypatch.setattr(job_settings, "sns_client", fake.client)

    with pytest.raises(ValueError, match="boom"), job_run("customers", ARGS, DATES):
        raise ValueError("boom")

    out = capsys.readouterr().out
    assert emitted_records(out)[0]["status"] == "FAILED"
    assert "ALERT_PUBLISH_FAILED ConnectionError: endpoint unreachable" in out


def test_sigterm_records_and_publishes_stopped(
    sns: FakeSns, with_topic: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """SIGTERM inside the run becomes JobStopped: a STOPPED record and alert, then re-raised."""
    with (
        pytest.raises(JobStopped),
        job_run("customers", ARGS, DATES) as record,
        phase(record, "transform"),
    ):
        os.kill(os.getpid(), signal.SIGTERM)

    [emitted] = emitted_records(capsys.readouterr().out)
    assert emitted["status"] == "STOPPED"
    assert emitted["error_type"] == "JobStopped"
    [alert] = sns.published
    assert alert["Subject"] == "[kpi_lakehouse prod] customers STOPPED in transform"
    assert "was stopped by hand or at its timeout in transform." in alert["Message"]


def test_sigterm_handler_is_restored_after_the_run(sns: FakeSns, without_topic: None) -> None:
    """The run's SIGTERM handler is only installed while the run is open."""
    before = signal.getsignal(signal.SIGTERM)

    with job_run("customers", ARGS, DATES):
        assert signal.getsignal(signal.SIGTERM) is not before

    assert signal.getsignal(signal.SIGTERM) is before


def test_alert_topic_arn_is_read_among_other_arguments() -> None:
    """The topic ARN is picked out of Glue's arguments; every other argument is ignored."""
    argv = ["--JOB_NAME", "x", "--ALERT_TOPIC_ARN", TOPIC_ARN, "--OUTPUT_PATH", "s3://b/"]

    assert alert_topic_arn(argv) == TOPIC_ARN
    assert alert_topic_arn(["--JOB_NAME", "x"]) is None


def test_subject_without_a_phase_and_cut_to_100_characters() -> None:
    """A run that failed before any step names no step, and a long subject is cut to 100."""
    short = RunRecord(job_name="customers", started_at="", processing_dates={}, status="FAILED")
    short.environment = "prod"
    long = RunRecord(job_name="x" * 120, started_at="", processing_dates={}, status="FAILED")

    assert alert_subject(short) == "[kpi_lakehouse prod] customers FAILED"
    assert len(alert_subject(long)) == 100


def test_message_gives_dates_run_id_and_console_link() -> None:
    """The body spans each source's dates, and links to the run in the Glue console."""
    record = RunRecord(
        job_name="customers",
        started_at="2026-11-11T06:00:00+00:00",
        processing_dates={"customers": ["2026-11-09", "2026-11-10"], "orders": ["2026-11-10"]},
        status="FAILED",
        phase="write",
        glue_job_name="kpi_lakehouse-prod-customers",
        glue_run_id="jr_abc123",
        environment="prod",
        error_type="ValueError",
        error_message="boom",
    )

    message = alert_message(record, "eu-central-1")

    assert "  customers: 2026-11-09 to 2026-11-10 (2 dates)" in message
    assert "  orders: 2026-11-10" in message
    assert "Run id: jr_abc123" in message
    assert (
        "Run: https://eu-central-1.console.aws.amazon.com/gluestudio/home?region=eu-central-1"
        "#/job/kpi_lakehouse-prod-customers/run/jr_abc123"
    ) in message
