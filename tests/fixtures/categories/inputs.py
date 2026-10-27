"""Input rows for the categories tests, one list per source.

Replace the example rows with 3-5 rows that exercise the transformations, then set
INPUTS_FILLED = True. Column names and types follow each source's contract.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

INPUTS_FILLED = True

# One export, on 18 October 2026, of the seven leaf categories on products and every ancestor
# they need to be flattened: 20 categories, as shop_simulator's engine.py numbers config.yaml's
# category tree. Paths are three levels deep (Department > Category > Leaf), except Running
# Shirts at four (Sport > Running > Running Apparel > Running Shirts). Sport and Home each
# have two branches. updated_at is the simulator's: 1000 days before its start, 2024-01-01.
CATEGORIES_ROWS = [
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0001",
        category_name="Women",
        parent_category_id=None,
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0002",
        category_name="Clothing",
        parent_category_id="CAT-0001",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0004",
        category_name="Tops",
        parent_category_id="CAT-0002",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0014",
        category_name="Men",
        parent_category_id=None,
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0015",
        category_name="Clothing",
        parent_category_id="CAT-0014",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0017",
        category_name="T-Shirts",
        parent_category_id="CAT-0015",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0023",
        category_name="Kids",
        parent_category_id=None,
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0024",
        category_name="Girls",
        parent_category_id="CAT-0023",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0025",
        category_name="Girls Clothing",
        parent_category_id="CAT-0024",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0030",
        category_name="Sport",
        parent_category_id=None,
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0031",
        category_name="Running",
        parent_category_id="CAT-0030",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0035",
        category_name="Running Apparel",
        parent_category_id="CAT-0031",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0036",
        category_name="Running Shirts",
        parent_category_id="CAT-0035",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0038",
        category_name="Fitness",
        parent_category_id="CAT-0030",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0039",
        category_name="Yoga Mats",
        parent_category_id="CAT-0038",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0044",
        category_name="Home",
        parent_category_id=None,
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0045",
        category_name="Kitchen",
        parent_category_id="CAT-0044",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0047",
        category_name="Knives",
        parent_category_id="CAT-0045",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0048",
        category_name="Living",
        parent_category_id="CAT-0044",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
    Row(
        snapshot_date=date(2026, 10, 18),
        category_id="CAT-0049",
        category_name="Cushions",
        parent_category_id="CAT-0048",
        is_active=True,
        updated_at=datetime(2021, 4, 6, 0, 0),
    ),
]
