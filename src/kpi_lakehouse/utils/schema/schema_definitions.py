"""One Spark StructType per data contract used by a rendered job.

Tool-owned and rebuilt whole from the contracts in `contracts/`. Edit the contracts, not
this file.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

categories_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("category_id", StringType(), nullable=False),
        StructField("category_name", StringType(), nullable=False),
        StructField("parent_category_id", StringType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
        StructField("category_id_level_1", StringType(), nullable=False),
        StructField("category_id_level_2", StringType(), nullable=True),
        StructField("category_id_level_3", StringType(), nullable=True),
        StructField("category_id_level_4", StringType(), nullable=True),
    ]
)

categories_raw_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("category_id", StringType(), nullable=False),
        StructField("category_name", StringType(), nullable=False),
        StructField("parent_category_id", StringType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

customers_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("email", StringType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=False),
        StructField("country_code", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=False),
        StructField("marketing_opt_in", BooleanType(), nullable=False),
        StructField("is_deleted", BooleanType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
        StructField("customer_version_id", StringType(), nullable=False),
        StructField("birth_year", IntegerType(), nullable=True),
    ]
)

customers_raw_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("email", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=False),
        StructField("country_code", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=False),
        StructField("marketing_opt_in", BooleanType(), nullable=False),
        StructField("is_deleted", BooleanType(), nullable=False),
    ]
)

dates_schema = StructType(
    [
        StructField("date", DateType(), nullable=False),
    ]
)

fx_rates_schema = StructType(
    [
        StructField("rate_date", DateType(), nullable=False),
        StructField("base_currency", StringType(), nullable=False),
        StructField("quote_currency", StringType(), nullable=False),
        StructField("rate", DecimalType(18, 8), nullable=False),
    ]
)

fx_rates_raw_schema = StructType(
    [
        StructField("rate_date", DateType(), nullable=False),
        StructField("base_currency", StringType(), nullable=False),
        StructField("quote_currency", StringType(), nullable=False),
        StructField("rate", DecimalType(18, 8), nullable=False),
    ]
)

marketing_spend_schema = StructType(
    [
        StructField("spend_date", DateType(), nullable=False),
        StructField("platform", StringType(), nullable=False),
        StructField("campaign_id", StringType(), nullable=False),
        StructField("campaign_name", StringType(), nullable=False),
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("impressions", LongType(), nullable=False),
        StructField("clicks", LongType(), nullable=False),
        StructField("spend_amount_eur", DecimalType(12, 2), nullable=False),
        StructField("currency_code", StringType(), nullable=False),
        StructField("reported_at", TimestampType(), nullable=False),
    ]
)

marketing_spend_raw_schema = StructType(
    [
        StructField("spend_date", DateType(), nullable=False),
        StructField("platform", StringType(), nullable=False),
        StructField("campaign_id", StringType(), nullable=False),
        StructField("campaign_name", StringType(), nullable=False),
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("impressions", LongType(), nullable=False),
        StructField("clicks", LongType(), nullable=False),
        StructField("spend_amount", DecimalType(12, 2), nullable=False),
        StructField("currency_code", StringType(), nullable=False),
        StructField("reported_at", TimestampType(), nullable=False),
    ]
)

order_items_schema = StructType(
    [
        StructField("order_date", DateType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("line_number", IntegerType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DecimalType(12, 2), nullable=False),
        StructField("discount_amount", DecimalType(12, 2), nullable=False),
        StructField("tax_rate", DecimalType(5, 4), nullable=False),
        StructField("rate", DecimalType(18, 8), nullable=False),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=False),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=False),
    ]
)

order_items_raw_schema = StructType(
    [
        StructField("order_date", DateType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("line_number", IntegerType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DecimalType(12, 2), nullable=False),
        StructField("discount_amount", DecimalType(12, 2), nullable=False),
        StructField("tax_rate", DecimalType(5, 4), nullable=False),
    ]
)

orders_schema = StructType(
    [
        StructField("order_date", DateType(), nullable=False),
        StructField("export_date", DateType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=False),
        StructField("order_status", StringType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
        StructField("currency_code", StringType(), nullable=False),
        StructField("voucher_code", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=False),
        StructField("shipping_country", StringType(), nullable=False),
        StructField("rate_date", DateType(), nullable=False),
        StructField("base_currency", StringType(), nullable=False),
        StructField("quote_currency", StringType(), nullable=False),
        StructField("rate", DecimalType(18, 8), nullable=False),
        StructField("shipping_fee_eur", DecimalType(12, 2), nullable=False),
    ]
)

orders_raw_schema = StructType(
    [
        StructField("export_date", DateType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=False),
        StructField("order_status", StringType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
        StructField("currency_code", StringType(), nullable=False),
        StructField("shipping_fee", DecimalType(12, 2), nullable=False),
        StructField("voucher_code", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=False),
        StructField("shipping_country", StringType(), nullable=False),
        StructField("is_test_order", BooleanType(), nullable=False),
    ]
)

products_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("sku", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("brand", StringType(), nullable=False),
        StructField("category_id", StringType(), nullable=False),
        StructField("list_price_eur", DecimalType(12, 2), nullable=False),
        StructField("cost_price_eur", DecimalType(12, 2), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("launched_date", DateType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
        StructField("product_version_id", StringType(), nullable=False),
    ]
)

products_raw_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("sku", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("brand", StringType(), nullable=False),
        StructField("category_id", StringType(), nullable=False),
        StructField("list_price_eur", DecimalType(12, 2), nullable=False),
        StructField("cost_price_eur", DecimalType(12, 2), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("launched_date", DateType(), nullable=False),
    ]
)

returns_schema = StructType(
    [
        StructField("update_date", DateType(), nullable=False),
        StructField("export_date", DateType(), nullable=False),
        StructField("return_id", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("line_number", IntegerType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("requested_ts", TimestampType(), nullable=False),
        StructField("quantity_returned", IntegerType(), nullable=False),
        StructField("return_reason", StringType(), nullable=False),
        StructField("return_status", StringType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("refund_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_refund_eur", DecimalType(12, 2), nullable=True),
    ]
)

returns_raw_schema = StructType(
    [
        StructField("export_date", DateType(), nullable=False),
        StructField("return_id", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("line_number", IntegerType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("requested_ts", TimestampType(), nullable=False),
        StructField("quantity_returned", IntegerType(), nullable=False),
        StructField("refund_amount", DecimalType(12, 2), nullable=True),
        StructField("return_reason", StringType(), nullable=False),
        StructField("return_status", StringType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

session_events_schema = StructType(
    [
        StructField("event_date", DateType(), nullable=False),
        StructField("event_id", StringType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("session_id", StringType(), nullable=False),
        StructField("anonymous_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("referrer_domain", StringType(), nullable=True),
        StructField("device_type", StringType(), nullable=False),
        StructField("country_code", StringType(), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
    ]
)

session_events_raw_schema = StructType(
    [
        StructField("event_date", DateType(), nullable=False),
        StructField("event_id", StringType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("session_id", StringType(), nullable=False),
        StructField("anonymous_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("referrer_domain", StringType(), nullable=True),
        StructField("device_type", StringType(), nullable=False),
        StructField("country_code", StringType(), nullable=True),
    ]
)

sessions_schema = StructType(
    [
        StructField("session_id", StringType(), nullable=False),
        StructField("event_date", DateType(), nullable=False),
        StructField("first_event_time", TimestampType(), nullable=False),
        StructField("last_event_time", TimestampType(), nullable=True),
        StructField("first_mkt_channel", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=False),
        StructField("last_event_type", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=True),
    ]
)
