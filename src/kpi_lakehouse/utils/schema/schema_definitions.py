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
        StructField(
            "category_id",
            StringType(),
            nullable=False,
            metadata={"primaryKey": True, "pattern": "^CAT-[0-9]{4}$"},
        ),
        StructField("category_name", StringType(), nullable=False),
        StructField(
            "parent_category_id",
            StringType(),
            nullable=True,
            metadata={"pattern": "^CAT-[0-9]{4}$"},
        ),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
        StructField("category_id_level_1", StringType(), nullable=False),
        StructField(
            "category_id_level_2",
            StringType(),
            nullable=True,
            metadata={"pattern": "^CAT-[0-9]{4}$"},
        ),
        StructField(
            "category_id_level_3",
            StringType(),
            nullable=True,
            metadata={"pattern": "^CAT-[0-9]{4}$"},
        ),
        StructField(
            "category_id_level_4",
            StringType(),
            nullable=True,
            metadata={"pattern": "^CAT-[0-9]{4}$"},
        ),
    ]
)

categories_raw_schema = StructType(
    [
        StructField(
            "category_id",
            StringType(),
            nullable=False,
            metadata={"primaryKey": True, "pattern": "^CAT-[0-9]{4}$"},
        ),
        StructField("category_name", StringType(), nullable=False),
        StructField(
            "parent_category_id",
            StringType(),
            nullable=True,
            metadata={"pattern": "^CAT-[0-9]{4}$"},
        ),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

customers_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField("customer_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField("email", StringType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=False),
        StructField("country_code", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField(
            "customer_segment",
            StringType(),
            nullable=False,
            metadata={"enum": ["standard", "plus", "business"]},
        ),
        StructField("marketing_opt_in", BooleanType(), nullable=False),
        StructField("is_deleted", BooleanType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
        StructField("customer_version_id", StringType(), nullable=False),
        StructField("birth_year", IntegerType(), nullable=True),
    ]
)

customers_raw_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField(
            "customer_id",
            StringType(),
            nullable=False,
            metadata={"primaryKey": True, "pattern": "^C[0-9]{7}$"},
        ),
        StructField("email", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=False),
        StructField(
            "country_code",
            StringType(),
            nullable=True,
            metadata={"pattern": "^[A-Z]{2}$"},
        ),
        StructField("city", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField(
            "customer_segment",
            StringType(),
            nullable=False,
            metadata={"enum": ["standard", "plus", "business"]},
        ),
        StructField("marketing_opt_in", BooleanType(), nullable=False),
        StructField("is_deleted", BooleanType(), nullable=False),
    ]
)

dates_schema = StructType(
    [
        StructField("date", DateType(), nullable=False),
    ]
)

dim_customer_schema = StructType(
    [
        StructField("valid_from", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField("customer_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField("country_code", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField(
            "customer_segment",
            StringType(),
            nullable=False,
            metadata={"enum": ["standard", "plus", "business"]},
        ),
        StructField("marketing_opt_in", BooleanType(), nullable=False),
        StructField("customer_version_id", StringType(), nullable=False),
        StructField("birth_year", IntegerType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("is_current", BooleanType(), nullable=False),
    ]
)

dim_dates_schema = StructType(
    [
        StructField("date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField("date_key", IntegerType(), nullable=False),
        StructField("year", IntegerType(), nullable=False),
        StructField(
            "quarter",
            IntegerType(),
            nullable=False,
            metadata={"minimum": "1", "maximum": "4"},
        ),
        StructField(
            "month",
            IntegerType(),
            nullable=False,
            metadata={"minimum": "1", "maximum": "12"},
        ),
        StructField(
            "iso_week",
            IntegerType(),
            nullable=False,
            metadata={"minimum": "1", "maximum": "53"},
        ),
        StructField(
            "weekday",
            IntegerType(),
            nullable=False,
            metadata={"minimum": "1", "maximum": "7"},
        ),
        StructField("is_weekend", BooleanType(), nullable=False),
        StructField("month_start", DateType(), nullable=False),
        StructField("month_end", DateType(), nullable=False),
    ]
)

dim_product_schema = StructType(
    [
        StructField("valid_from", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField("product_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField("sku", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("brand", StringType(), nullable=False),
        StructField("category_id", StringType(), nullable=False),
        StructField("list_price_eur", DecimalType(12, 2), nullable=False),
        StructField("cost_price_eur", DecimalType(12, 2), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("launched_date", DateType(), nullable=False),
        StructField("product_version_id", StringType(), nullable=False),
        StructField("valid_to", DateType(), nullable=True),
        StructField("is_current", BooleanType(), nullable=False),
        StructField("category_id_level_1", StringType(), nullable=False),
        StructField("category_id_level_2", StringType(), nullable=True),
        StructField("category_id_level_3", StringType(), nullable=True),
        StructField("category_id_level_4", StringType(), nullable=True),
    ]
)

fact_order_line_schema = StructType(
    [
        StructField("order_date", DateType(), nullable=False),
        StructField("order_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField(
            "line_number",
            IntegerType(),
            nullable=False,
            metadata={"primaryKey": True, "minimum": "1"},
        ),
        StructField("customer_id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=False),
        StructField(
            "order_status",
            StringType(),
            nullable=False,
            metadata={"enum": ["placed", "paid", "shipped", "delivered", "cancelled"]},
        ),
        StructField("product_id", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False, metadata={"minimum": "1"}),
        StructField("unit_price", DecimalType(12, 2), nullable=False),
        StructField("discount_amount", DecimalType(12, 2), nullable=False),
        StructField("tax_rate", DecimalType(5, 4), nullable=False),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("product_version_id", StringType(), nullable=True),
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=False, metadata={"minimum": "0"}),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=False),
        StructField("net_revenue_after_returns_eur", DecimalType(12, 2), nullable=False),
        StructField(
            "mkt_channel",
            StringType(),
            nullable=False,
            metadata={
                "enum": [
                    "paid_search",
                    "paid_social",
                    "affiliate",
                    "email",
                    "organic_search",
                    "referral",
                    "direct",
                ],
            },
        ),
        StructField("date_key", IntegerType(), nullable=False),
    ]
)

fact_session_schema = StructType(
    [
        StructField("session_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField("event_date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField("first_event_time", TimestampType(), nullable=False),
        StructField("last_event_time", TimestampType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField(
            "last_event_type",
            StringType(),
            nullable=False,
            metadata={
                "enum": ["page_view", "product_view", "add_to_cart", "begin_checkout", "purchase"],
            },
        ),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=False),
        StructField("converted_customer", BooleanType(), nullable=False),
        StructField(
            "mkt_channel",
            StringType(),
            nullable=True,
            metadata={
                "enum": [
                    "paid_search",
                    "paid_social",
                    "affiliate",
                    "email",
                    "organic_search",
                    "referral",
                    "direct",
                ],
            },
        ),
    ]
)

fx_rates_schema = StructType(
    [
        StructField("rate_date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField("base_currency", StringType(), nullable=False, metadata={"enum": ["EUR"]}),
        StructField("quote_currency", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField("rate", DecimalType(18, 8), nullable=False),
    ]
)

fx_rates_raw_schema = StructType(
    [
        StructField("rate_date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField("base_currency", StringType(), nullable=False, metadata={"enum": ["EUR"]}),
        StructField(
            "quote_currency",
            StringType(),
            nullable=False,
            metadata={"primaryKey": True, "pattern": "^[A-Z]{3}$"},
        ),
        StructField("rate", DecimalType(18, 8), nullable=False, metadata={"exclusiveMinimum": "0"}),
    ]
)

kpi_channel_performance_schema = StructType(
    [
        StructField("date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField(
            "mkt_channel",
            StringType(),
            nullable=True,
            metadata={
                "primaryKey": True,
                "enum": [
                    "paid_search",
                    "paid_social",
                    "affiliate",
                    "email",
                    "organic_search",
                    "referral",
                    "direct",
                ],
            },
        ),
        StructField("sessions", LongType(), nullable=False, metadata={"minimum": "0"}),
        StructField("converted_sessions", LongType(), nullable=False, metadata={"minimum": "0"}),
        StructField("attributed_net_revenue_eur", DecimalType(12, 2), nullable=False),
        StructField("spend_eur", DecimalType(12, 2), nullable=False),
        StructField("date_key", IntegerType(), nullable=False),
        StructField("conversion_rate", DecimalType(5, 4), nullable=True),
        StructField("roas", DecimalType(12, 2), nullable=True),
    ]
)

kpi_daily_sales_schema = StructType(
    [
        StructField("order_date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField("orders", IntegerType(), nullable=False, metadata={"minimum": "0"}),
        StructField("cancelled_orders", IntegerType(), nullable=False, metadata={"minimum": "0"}),
        StructField("units", IntegerType(), nullable=True, metadata={"minimum": "0"}),
        StructField("gmv_eur", DecimalType(12, 2), nullable=True, metadata={"minimum": "0"}),
        StructField("net_sales_eur", DecimalType(12, 2), nullable=True, metadata={"minimum": "0"}),
        StructField("net_revenue_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("date_key", IntegerType(), nullable=False),
        StructField("aov_eur", DecimalType(12, 2), nullable=True, metadata={"minimum": "0"}),
        StructField("gross_margin_pct", DecimalType(2, 2), nullable=True),
        StructField(
            "cancellation_rate",
            DecimalType(2, 2),
            nullable=True,
            metadata={"minimum": "0"},
        ),
    ]
)

kpi_return_rate_schema = StructType(
    [
        StructField("month", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField(
            "category_id_level_1",
            StringType(),
            nullable=True,
            metadata={"primaryKey": True},
        ),
        StructField("units_sold", IntegerType(), nullable=False, metadata={"minimum": "0"}),
        StructField("units_returned", IntegerType(), nullable=False, metadata={"minimum": "0"}),
        StructField("net_refunds_eur", DecimalType(12, 2), nullable=False),
        StructField("return_rate", DecimalType(5, 4), nullable=True),
        StructField("is_final", BooleanType(), nullable=False),
    ]
)

marketing_spend_schema = StructType(
    [
        StructField("spend_date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField(
            "platform",
            StringType(),
            nullable=False,
            metadata={
                "primaryKey": True,
                "enum": ["google_ads", "meta", "tiktok", "affiliate_network", "newsletter_tool"],
            },
        ),
        StructField("campaign_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField("campaign_name", StringType(), nullable=False),
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("impressions", LongType(), nullable=False),
        StructField("clicks", LongType(), nullable=False),
        StructField("spend_amount_eur", DecimalType(12, 2), nullable=False),
        StructField("currency_code", StringType(), nullable=False),
        StructField("reported_at", TimestampType(), nullable=False, metadata={"primaryKey": True}),
    ]
)

marketing_spend_raw_schema = StructType(
    [
        StructField("spend_date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField(
            "platform",
            StringType(),
            nullable=False,
            metadata={
                "primaryKey": True,
                "enum": ["google_ads", "meta", "tiktok", "affiliate_network", "newsletter_tool"],
            },
        ),
        StructField("campaign_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField("campaign_name", StringType(), nullable=False),
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("impressions", LongType(), nullable=False, metadata={"minimum": "0"}),
        StructField("clicks", LongType(), nullable=False, metadata={"minimum": "0"}),
        StructField("spend_amount", DecimalType(12, 2), nullable=False, metadata={"minimum": "0"}),
        StructField(
            "currency_code",
            StringType(),
            nullable=False,
            metadata={"pattern": "^[A-Z]{3}$"},
        ),
        StructField("reported_at", TimestampType(), nullable=False, metadata={"primaryKey": True}),
    ]
)

order_items_schema = StructType(
    [
        StructField("order_date", DateType(), nullable=False),
        StructField("order_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField(
            "line_number",
            IntegerType(),
            nullable=False,
            metadata={"primaryKey": True, "minimum": "1"},
        ),
        StructField(
            "product_id",
            StringType(),
            nullable=False,
            metadata={"pattern": "^P[0-9]{5}$"},
        ),
        StructField("quantity", IntegerType(), nullable=False, metadata={"minimum": "1"}),
        StructField("unit_price", DecimalType(12, 2), nullable=False),
        StructField("discount_amount", DecimalType(12, 2), nullable=False),
        StructField("tax_rate", DecimalType(5, 4), nullable=False),
        StructField("rate", DecimalType(18, 8), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
    ]
)

order_items_raw_schema = StructType(
    [
        StructField("order_date", DateType(), nullable=False),
        StructField(
            "order_id",
            StringType(),
            nullable=False,
            metadata={"primaryKey": True, "pattern": "^ORD-[0-9]{9}$"},
        ),
        StructField(
            "line_number",
            IntegerType(),
            nullable=False,
            metadata={"primaryKey": True, "minimum": "1"},
        ),
        StructField(
            "product_id",
            StringType(),
            nullable=False,
            metadata={"pattern": "^P[0-9]{5}$"},
        ),
        StructField("quantity", IntegerType(), nullable=False, metadata={"minimum": "1"}),
        StructField("unit_price", DecimalType(12, 2), nullable=False, metadata={"minimum": "0"}),
        StructField(
            "discount_amount",
            DecimalType(12, 2),
            nullable=False,
            metadata={"minimum": "0"},
        ),
        StructField(
            "tax_rate",
            DecimalType(5, 4),
            nullable=False,
            metadata={"minimum": "0", "maximum": "1"},
        ),
    ]
)

orders_schema = StructType(
    [
        StructField("order_date", DateType(), nullable=False),
        StructField("export_date", DateType(), nullable=False),
        StructField("order_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField("customer_id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=False),
        StructField(
            "order_status",
            StringType(),
            nullable=False,
            metadata={"enum": ["placed", "paid", "shipped", "delivered", "cancelled"]},
        ),
        StructField("updated_at", TimestampType(), nullable=False, metadata={"primaryKey": True}),
        StructField(
            "currency_code",
            StringType(),
            nullable=False,
            metadata={"enum": ["EUR", "CHF", "PLN", "SEK", "DKK"]},
        ),
        StructField("voucher_code", StringType(), nullable=True),
        StructField(
            "payment_method",
            StringType(),
            nullable=False,
            metadata={"enum": ["card", "paypal", "klarna_invoice", "bank_transfer"]},
        ),
        StructField("shipping_country", StringType(), nullable=False),
        StructField("rate_date", DateType(), nullable=False),
        StructField("base_currency", StringType(), nullable=False, metadata={"enum": ["EUR"]}),
        StructField(
            "quote_currency",
            StringType(),
            nullable=False,
            metadata={"enum": ["EUR", "CHF", "PLN", "SEK", "DKK"]},
        ),
        StructField("rate", DecimalType(18, 8), nullable=False),
        StructField("shipping_fee_eur", DecimalType(12, 2), nullable=False),
    ]
)

orders_raw_schema = StructType(
    [
        StructField("export_date", DateType(), nullable=False),
        StructField(
            "order_id",
            StringType(),
            nullable=False,
            metadata={
                "primaryKey": True,
                "minLength": 13,
                "maxLength": 13,
                "pattern": "^ORD-[0-9]{9}$",
            },
        ),
        StructField(
            "customer_id",
            StringType(),
            nullable=False,
            metadata={"minLength": 8, "maxLength": 8, "pattern": "^C[0-9]{7}$"},
        ),
        StructField(
            "session_id",
            StringType(),
            nullable=True,
            metadata={"minLength": 32, "maxLength": 32, "pattern": "^[0-9a-f]{32}$"},
        ),
        StructField(
            "order_ts",
            TimestampType(),
            nullable=False,
            metadata={"format": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "timezone": True},
        ),
        StructField(
            "order_status",
            StringType(),
            nullable=False,
            metadata={"enum": ["placed", "paid", "shipped", "delivered", "cancelled"]},
        ),
        StructField(
            "updated_at",
            TimestampType(),
            nullable=False,
            metadata={
                "primaryKey": True,
                "format": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
                "timezone": True,
            },
        ),
        StructField(
            "currency_code",
            StringType(),
            nullable=False,
            metadata={"enum": ["EUR", "DKK", "PLN", "CHF", "SEK"], "minLength": 3, "maxLength": 3},
        ),
        StructField("shipping_fee", DecimalType(12, 2), nullable=False, metadata={"minimum": "0"}),
        StructField("voucher_code", StringType(), nullable=True),
        StructField(
            "payment_method",
            StringType(),
            nullable=False,
            metadata={"enum": ["card", "paypal", "klarna_invoice", "bank_transfer"]},
        ),
        StructField(
            "shipping_country",
            StringType(),
            nullable=False,
            metadata={"minLength": 2, "maxLength": 2, "pattern": "^[A-Z]{2}$"},
        ),
        StructField("is_test_order", BooleanType(), nullable=False),
    ]
)

products_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField("product_id", StringType(), nullable=False, metadata={"primaryKey": True}),
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
        StructField("snapshot_date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField(
            "product_id",
            StringType(),
            nullable=False,
            metadata={"primaryKey": True, "pattern": "^P[0-9]{5}$"},
        ),
        StructField(
            "sku",
            StringType(),
            nullable=False,
            metadata={"pattern": "^SKU-[A-Z]{2}-[0-9]{6}$"},
        ),
        StructField("product_name", StringType(), nullable=False),
        StructField("brand", StringType(), nullable=False),
        StructField(
            "category_id",
            StringType(),
            nullable=False,
            metadata={"pattern": "^CAT-[0-9]{4}$"},
        ),
        StructField(
            "list_price_eur",
            DecimalType(12, 2),
            nullable=False,
            metadata={"minimum": "0"},
        ),
        StructField(
            "cost_price_eur",
            DecimalType(12, 2),
            nullable=False,
            metadata={"minimum": "0"},
        ),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("launched_date", DateType(), nullable=False),
    ]
)

returns_schema = StructType(
    [
        StructField("update_date", DateType(), nullable=False),
        StructField("export_date", DateType(), nullable=False),
        StructField("return_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField("order_id", StringType(), nullable=False),
        StructField("line_number", IntegerType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("requested_ts", TimestampType(), nullable=False),
        StructField("quantity_returned", IntegerType(), nullable=False, metadata={"minimum": "1"}),
        StructField(
            "return_reason",
            StringType(),
            nullable=False,
            metadata={
                "enum": ["wrong_size", "damaged", "not_as_described", "changed_mind", "other"],
            },
        ),
        StructField(
            "return_status",
            StringType(),
            nullable=False,
            metadata={"enum": ["requested", "received", "refunded", "rejected"]},
        ),
        StructField("updated_at", TimestampType(), nullable=False, metadata={"primaryKey": True}),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("refund_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_refund_eur", DecimalType(12, 2), nullable=True),
    ]
)

returns_raw_schema = StructType(
    [
        StructField("export_date", DateType(), nullable=False),
        StructField(
            "return_id",
            StringType(),
            nullable=False,
            metadata={"primaryKey": True, "pattern": "^RET-[0-9]{8}$"},
        ),
        StructField(
            "order_id",
            StringType(),
            nullable=False,
            metadata={"pattern": "^ORD-[0-9]{9}$"},
        ),
        StructField("line_number", IntegerType(), nullable=False, metadata={"minimum": "1"}),
        StructField(
            "product_id",
            StringType(),
            nullable=False,
            metadata={"pattern": "^P[0-9]{5}$"},
        ),
        StructField("requested_ts", TimestampType(), nullable=False),
        StructField("quantity_returned", IntegerType(), nullable=False, metadata={"minimum": "1"}),
        StructField("refund_amount", DecimalType(12, 2), nullable=True),
        StructField(
            "return_reason",
            StringType(),
            nullable=False,
            metadata={
                "enum": ["wrong_size", "damaged", "not_as_described", "changed_mind", "other"],
            },
        ),
        StructField(
            "return_status",
            StringType(),
            nullable=False,
            metadata={"enum": ["requested", "received", "refunded", "rejected"]},
        ),
        StructField("updated_at", TimestampType(), nullable=False, metadata={"primaryKey": True}),
    ]
)

session_events_schema = StructType(
    [
        StructField("event_date", DateType(), nullable=False),
        StructField("event_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("session_id", StringType(), nullable=False),
        StructField("anonymous_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField(
            "event_type",
            StringType(),
            nullable=False,
            metadata={
                "enum": ["page_view", "product_view", "add_to_cart", "begin_checkout", "purchase"],
            },
        ),
        StructField("product_id", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("referrer_domain", StringType(), nullable=True),
        StructField(
            "device_type",
            StringType(),
            nullable=False,
            metadata={"enum": ["desktop", "mobile_web", "ios_app", "android_app"]},
        ),
        StructField("country_code", StringType(), nullable=True),
        StructField(
            "mkt_channel",
            StringType(),
            nullable=True,
            metadata={
                "enum": [
                    "paid_search",
                    "paid_social",
                    "affiliate",
                    "email",
                    "organic_search",
                    "referral",
                    "direct",
                ],
            },
        ),
    ]
)

session_events_raw_schema = StructType(
    [
        StructField("event_date", DateType(), nullable=False),
        StructField(
            "event_id",
            StringType(),
            nullable=False,
            metadata={"primaryKey": True, "pattern": "^[0-9a-f]{32}$"},
        ),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField(
            "session_id",
            StringType(),
            nullable=False,
            metadata={"pattern": "^[0-9a-f]{32}$"},
        ),
        StructField(
            "anonymous_id",
            StringType(),
            nullable=False,
            metadata={"pattern": "^[0-9a-f]{16}$"},
        ),
        StructField(
            "customer_id",
            StringType(),
            nullable=True,
            metadata={"pattern": "^C[0-9]{7}$"},
        ),
        StructField(
            "event_type",
            StringType(),
            nullable=False,
            metadata={
                "enum": ["page_view", "product_view", "add_to_cart", "begin_checkout", "purchase"],
            },
        ),
        StructField("product_id", StringType(), nullable=True, metadata={"pattern": "^P[0-9]{5}$"}),
        StructField(
            "order_id",
            StringType(),
            nullable=True,
            metadata={"pattern": "^ORD-[0-9]{9}$"},
        ),
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("referrer_domain", StringType(), nullable=True),
        StructField(
            "device_type",
            StringType(),
            nullable=False,
            metadata={"enum": ["desktop", "mobile_web", "ios_app", "android_app"]},
        ),
        StructField(
            "country_code",
            StringType(),
            nullable=True,
            metadata={"pattern": "^[A-Z]{2}$"},
        ),
    ]
)

sessions_schema = StructType(
    [
        StructField("session_id", StringType(), nullable=False, metadata={"primaryKey": True}),
        StructField("event_date", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField("first_event_time", TimestampType(), nullable=False),
        StructField("last_event_time", TimestampType(), nullable=True),
        StructField(
            "first_mkt_channel",
            StringType(),
            nullable=True,
            metadata={
                "enum": [
                    "paid_search",
                    "paid_social",
                    "affiliate",
                    "email",
                    "organic_search",
                    "referral",
                    "direct",
                ],
            },
        ),
        StructField("customer_id", StringType(), nullable=True),
        StructField(
            "last_event_type",
            StringType(),
            nullable=False,
            metadata={
                "enum": ["page_view", "product_view", "add_to_cart", "begin_checkout", "purchase"],
            },
        ),
        StructField("order_id", StringType(), nullable=True),
    ]
)
