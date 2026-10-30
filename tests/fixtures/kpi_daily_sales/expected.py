"""Expected output rows of the kpi_daily_sales transformations.

One list per transformation. `engineering-automation generate expected-outputs` drafts
each empty list from the transformation's description and input rows, without seeing its
code. Check every row, correct it, then set that transformation's REVIEWED flag to True.
Its test fails until you do.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

AGGREGATE_DAILY_TOTALS_REVIEWED = True
AGGREGATE_DAILY_TOTALS_ROWS: list[Row] = [
    Row(
        order_date=date(2026, 10, 15),
        orders=1,
        cancelled_orders=1,
        units=4,
        gmv_eur=Decimal("44.96"),
        net_sales_eur=Decimal("37.79"),
        net_revenue_eur=Decimal("37.79"),
        gross_margin_eur=None,
    ),
    Row(
        order_date=date(2026, 10, 16),
        orders=1,
        cancelled_orders=0,
        units=2,
        gmv_eur=Decimal("45.72"),
        net_sales_eur=Decimal("42.29"),
        net_revenue_eur=Decimal("18.50"),
        gross_margin_eur=None,
    ),
    Row(
        order_date=date(2026, 10, 17),
        orders=1,
        cancelled_orders=0,
        units=1,
        gmv_eur=Decimal("32.34"),
        net_sales_eur=Decimal("26.29"),
        net_revenue_eur=Decimal("26.29"),
        gross_margin_eur=Decimal("10.12"),
    ),
]

CREATE_DATE_KEY_REVIEWED = True
CREATE_DATE_KEY_ROWS: list[Row] = [
    Row(
        order_date=date(2026, 10, 15),
        orders=1,
        cancelled_orders=1,
        units=4,
        gmv_eur=Decimal("44.96"),
        net_sales_eur=Decimal("37.79"),
        net_revenue_eur=Decimal("37.79"),
        gross_margin_eur=None,
        date_key=20261015,
    ),
    Row(
        order_date=date(2026, 10, 16),
        orders=1,
        cancelled_orders=0,
        units=2,
        gmv_eur=Decimal("45.72"),
        net_sales_eur=Decimal("42.29"),
        net_revenue_eur=Decimal("18.50"),
        gross_margin_eur=None,
        date_key=20261016,
    ),
    Row(
        order_date=date(2026, 10, 17),
        orders=1,
        cancelled_orders=0,
        units=1,
        gmv_eur=Decimal("32.34"),
        net_sales_eur=Decimal("26.29"),
        net_revenue_eur=Decimal("26.29"),
        gross_margin_eur=Decimal("10.12"),
        date_key=20261017,
    ),
]

CREATE_AVERAGE_ORDER_VALUE_COLUMN_REVIEWED = True
CREATE_AVERAGE_ORDER_VALUE_COLUMN_ROWS: list[Row] = [
    Row(
        order_date=date(2026, 10, 15),
        orders=1,
        cancelled_orders=1,
        units=4,
        gmv_eur=Decimal("44.96"),
        net_sales_eur=Decimal("37.79"),
        net_revenue_eur=Decimal("37.79"),
        gross_margin_eur=None,
        date_key=20261015,
        aov_eur=Decimal("37.79"),
    ),
    Row(
        order_date=date(2026, 10, 16),
        orders=1,
        cancelled_orders=0,
        units=2,
        gmv_eur=Decimal("45.72"),
        net_sales_eur=Decimal("42.29"),
        net_revenue_eur=Decimal("18.50"),
        gross_margin_eur=None,
        date_key=20261016,
        aov_eur=Decimal("18.50"),
    ),
    Row(
        order_date=date(2026, 10, 17),
        orders=1,
        cancelled_orders=0,
        units=1,
        gmv_eur=Decimal("32.34"),
        net_sales_eur=Decimal("26.29"),
        net_revenue_eur=Decimal("26.29"),
        gross_margin_eur=Decimal("10.12"),
        date_key=20261017,
        aov_eur=Decimal("26.29"),
    ),
]

CREATE_GROSS_MARGIN_PERCENTAGE_COLUMN_REVIEWED = True
CREATE_GROSS_MARGIN_PERCENTAGE_COLUMN_ROWS: list[Row] = [
    Row(
        order_date=date(2026, 10, 15),
        orders=1,
        cancelled_orders=1,
        units=4,
        gmv_eur=Decimal("44.96"),
        net_sales_eur=Decimal("37.79"),
        net_revenue_eur=Decimal("37.79"),
        gross_margin_eur=None,
        date_key=20261015,
        aov_eur=Decimal("37.79"),
        gross_margin_pct=None,
    ),
    Row(
        order_date=date(2026, 10, 16),
        orders=1,
        cancelled_orders=0,
        units=2,
        gmv_eur=Decimal("45.72"),
        net_sales_eur=Decimal("42.29"),
        net_revenue_eur=Decimal("18.50"),
        gross_margin_eur=None,
        date_key=20261016,
        aov_eur=Decimal("18.50"),
        gross_margin_pct=None,
    ),
    Row(
        order_date=date(2026, 10, 17),
        orders=1,
        cancelled_orders=0,
        units=1,
        gmv_eur=Decimal("32.34"),
        net_sales_eur=Decimal("26.29"),
        net_revenue_eur=Decimal("26.29"),
        gross_margin_eur=Decimal("10.12"),
        date_key=20261017,
        aov_eur=Decimal("26.29"),
        gross_margin_pct=Decimal("0.38"),
    ),
]

CREATE_CANCELLATION_RATE_COLUMN_REVIEWED = True
CREATE_CANCELLATION_RATE_COLUMN_ROWS: list[Row] = [
    Row(
        order_date=date(2026, 10, 15),
        orders=1,
        cancelled_orders=1,
        units=4,
        gmv_eur=Decimal("44.96"),
        net_sales_eur=Decimal("37.79"),
        net_revenue_eur=Decimal("37.79"),
        gross_margin_eur=None,
        date_key=20261015,
        aov_eur=Decimal("37.79"),
        gross_margin_pct=None,
        cancellation_rate=Decimal("0.50"),
    ),
    Row(
        order_date=date(2026, 10, 16),
        orders=1,
        cancelled_orders=0,
        units=2,
        gmv_eur=Decimal("45.72"),
        net_sales_eur=Decimal("42.29"),
        net_revenue_eur=Decimal("18.50"),
        gross_margin_eur=None,
        date_key=20261016,
        aov_eur=Decimal("18.50"),
        gross_margin_pct=None,
        cancellation_rate=Decimal("0.00"),
    ),
    Row(
        order_date=date(2026, 10, 17),
        orders=1,
        cancelled_orders=0,
        units=1,
        gmv_eur=Decimal("32.34"),
        net_sales_eur=Decimal("26.29"),
        net_revenue_eur=Decimal("26.29"),
        gross_margin_eur=Decimal("10.12"),
        date_key=20261017,
        aov_eur=Decimal("26.29"),
        gross_margin_pct=Decimal("0.38"),
        cancellation_rate=Decimal("0.00"),
    ),
]
