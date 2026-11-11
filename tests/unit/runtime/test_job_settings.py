"""The run record, the alert a run that doesn't succeed publishes, and the run watcher.

The SNS and Glue clients are replaced by fakes, and the watcher reads the run every 10 ms,
so these tests need no AWS account and no Spark session.
"""

import json
import sys
import time
from argparse import Namespace
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from kpi_lakehouse.runtime import job_settings
from kpi_lakehouse.runtime.job_settings import (
    RUN_RECORD_PREFIX,
    RunRecord,
    alert_message,
    alert_subject,
    alert_topic_arn,
    job_run,
    phase,
    run_outcome,
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


class FakeGlue:
    """Answers GetJobRun with the run's `state`, `started` and `timeout`, or raises `error`."""

    def __init__(self) -> None:
        self.state = "RUNNING"
        self.started = datetime.now(UTC)
        self.timeout = 60
        self.error: Exception | None = None
        self.calls: list[tuple[str, str]] = []

    def client(self, region: str) -> "FakeGlue":
        return self

    def get_job_run(self, *, JobName: str, RunId: str) -> Any:
        self.calls.append((JobName, RunId))
        if self.error is not None:
            raise self.error
        return {
            "JobRun": {
                "JobRunState": self.state,
                "StartedOn": self.started,
                "Timeout": self.timeout,
            }
        }


@pytest.fixture(autouse=True)
def glue(monkeypatch: pytest.MonkeyPatch) -> FakeGlue:
    """A fake Glue client in place of boto3's, read every 10 ms instead of every 15 s."""
    fake = FakeGlue()
    monkeypatch.setattr(job_settings, "glue_client", fake.client)
    monkeypatch.setattr(job_settings, "WATCH_INTERVAL_SECONDS", 0.01)
    return fake


def wait_for(condition: Callable[[], Any], seconds: float = 2.0) -> None:
    """Wait until the watcher thread has made `condition` true."""
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline, "the watcher did nothing in time"
        time.sleep(0.01)


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


def test_watcher_records_and_publishes_stopped_once(
    sns: FakeSns, glue: FakeGlue, with_topic: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """A run Glue is stopping is recorded and alerted as STOPPED, and gets no second record."""
    glue.state = "STOPPING"

    with job_run("customers", ARGS, DATES) as record, phase(record, "read"):
        wait_for(lambda: sns.published)

    [emitted] = emitted_records(capsys.readouterr().out)
    assert emitted["status"] == "STOPPED"
    assert emitted["phase"] == "read"
    [alert] = sns.published
    assert alert["Subject"] == "[kpi_lakehouse prod] customers STOPPED in read"
    assert "was stopped in read." in alert["Message"]
    assert glue.calls == [("kpi_lakehouse-prod-customers", "jr_abc123")] * len(glue.calls)


def test_watcher_records_timeout_before_glue_ends_the_run(
    sns: FakeSns, glue: FakeGlue, with_topic: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """A run within a minute of its Glue timeout is recorded and alerted as TIMEOUT."""
    glue.started = datetime.now(UTC) - timedelta(minutes=2, seconds=30)
    glue.timeout = 3

    with job_run("customers", ARGS, DATES) as record, phase(record, "write"):
        wait_for(lambda: sns.published)

    [emitted] = emitted_records(capsys.readouterr().out)
    assert emitted["status"] == "TIMEOUT"
    [alert] = sns.published
    assert alert["Subject"] == "[kpi_lakehouse prod] customers TIMEOUT in write"
    assert "3-minute timeout" in alert["Message"]


def test_watcher_failing_to_read_the_run_never_fails_it(
    sns: FakeSns, glue: FakeGlue, with_topic: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """A Glue read that raises is printed, and the run still ends with its own record."""
    glue.error = ConnectionError("endpoint unreachable")

    with job_run("customers", ARGS, DATES):
        wait_for(lambda: glue.calls)

    out = capsys.readouterr().out
    assert "RUN_WATCH_FAILED ConnectionError: endpoint unreachable" in out
    assert [r["status"] for r in emitted_records(out)] == ["SUCCEEDED"]


def test_no_watcher_without_topic(sns: FakeSns, glue: FakeGlue, without_topic: None) -> None:
    """Without --ALERT_TOPIC_ARN, as in a local run, Glue is never read."""
    glue.state = "STOPPING"

    with job_run("customers", ARGS, DATES):
        time.sleep(0.1)

    assert glue.calls == []
    assert sns.published == []


def test_run_outcome_reads_state_and_timeout() -> None:
    """STOPPING and STOPPED mean stopped; the last minute before the timeout means timing out."""
    started = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)
    running = {"JobRunState": "RUNNING", "StartedOn": started, "Timeout": 60}

    assert run_outcome(running, started + timedelta(minutes=58)) is None
    assert run_outcome(running, started + timedelta(minutes=59))[0] == "TIMEOUT"  # type: ignore[index]
    for state in ("STOPPING", "STOPPED"):
        assert run_outcome({**running, "JobRunState": state}, started)[0] == "STOPPED"  # type: ignore[index]


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
