"""Input rows for the marketing_spend tests, one list per source.

Replace the example rows with 3-5 rows that exercise the transformations, then set
INPUTS_FILLED = True. Column names and types follow each source's contract.

A source that reads another job's output takes that job's expected output rows, imported
from its fixtures below with their REVIEWED flag; the tests fail until they are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

from tests.fixtures.fx_rates.expected import COALESCE_RATE_DATE_REVIEWED
from tests.fixtures.fx_rates.expected import COALESCE_RATE_DATE_ROWS as FX_RATES_ROWS

INPUTS_FILLED = True

# Spend for the two campaigns whose UTMs appear in the session_events fixture, shaped like
# shop_simulator's engine.py and config.yaml: reported after midnight, a minute apart per
# campaign, and restated the next night with clicks and spend moved by up to 4%. Both are
# EUR accounts, which the fx_rates output covers on every date; the simulator's only other
# account currency, USD, has no rate there. A third campaign, GA-1003, is a CHF account not in
# the simulator's config, added so spend converts at a rate other than 1.0.
MARKETING_SPEND_ROWS = [
    # GA-1002 (google / cpc, as ORD-000004812's session) for the 15th, first reported at
    # 00:06 on the 16th, then restated at 00:31 on the 17th: same spend_date, platform and
    # campaign_id, so keep latest restatement keeps only the second.
    Row(
        spend_date=date(2026, 10, 15),
        platform="google_ads",
        campaign_id="GA-1002",
        campaign_name="Generic Shopping",
        utm_source="google",
        utm_medium="cpc",
        utm_campaign="generic_shopping",
        impressions=14273,
        clicks=309,
        spend_amount=Decimal("197.33"),
        currency_code="EUR",
        reported_at=datetime(2026, 10, 16, 0, 6),
    ),
    Row(
        spend_date=date(2026, 10, 15),
        platform="google_ads",
        campaign_id="GA-1002",
        campaign_name="Generic Shopping",
        utm_source="google",
        utm_medium="cpc",
        utm_campaign="generic_shopping",
        impressions=14273,
        clicks=304,
        spend_amount=Decimal("193.98"),
        currency_code="EUR",
        reported_at=datetime(2026, 10, 17, 0, 31),
    ),
    # AF-4001 (awin / affiliate, as ORD-000004814's session) for the 16th, reported once.
    Row(
        spend_date=date(2026, 10, 16),
        platform="affiliate_network",
        campaign_id="AF-4001",
        campaign_name="Voucher Partners",
        utm_source="awin",
        utm_medium="affiliate",
        utm_campaign="voucher_partners",
        impressions=4541,
        clicks=131,
        spend_amount=Decimal("27.96"),
        currency_code="EUR",
        reported_at=datetime(2026, 10, 17, 0, 10),
    ),
    # GA-1003, a Swiss Google Ads account billed in CHF, for the 16th: not in the simulator's
    # config, which has no CHF account. Converts at the 16th's CHF rate, 0.9445.
    Row(
        spend_date=date(2026, 10, 16),
        platform="google_ads",
        campaign_id="GA-1003",
        campaign_name="Generic Shopping CH",
        utm_source="google",
        utm_medium="cpc",
        utm_campaign="generic_shopping_ch",
        impressions=2817,
        clicks=58,
        spend_amount=Decimal("36.19"),
        currency_code="CHF",
        reported_at=datetime(2026, 10, 17, 0, 12),
    ),
]
