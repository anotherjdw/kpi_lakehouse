"""Input rows for the sessions tests, one list per source.

Every source reads another job's output, so its rows are that job's expected output rows,
imported from its fixtures below with their REVIEWED flag. There is nothing to fill in
here; the tests fail until those rows are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

from tests.fixtures.session_events.expected import DERIVE_MARKETING_CHANNEL_REVIEWED
from tests.fixtures.session_events.expected import (
    DERIVE_MARKETING_CHANNEL_ROWS as SESSION_EVENTS_ROWS,
)
