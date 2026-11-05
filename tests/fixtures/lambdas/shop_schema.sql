-- The shop backend's tables, as the extract Lambda reads them. A copy of the shop simulator's
-- schema.sql (e-commerce-model/shop_simulator) without its CREATE USER and GRANT statements,
-- which a local test database does not need. Keep it in step with that file.

CREATE TABLE IF NOT EXISTS categories (
    category_id text NOT NULL,
    category_name text NOT NULL,
    parent_category_id text,
    is_active boolean NOT NULL,
    updated_at timestamp NOT NULL,
    _ingested_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (category_id)
);

CREATE INDEX IF NOT EXISTS categories_ingested_at ON categories (_ingested_at);

CREATE TABLE IF NOT EXISTS products (
    product_id text NOT NULL,
    sku text NOT NULL,
    product_name text NOT NULL,
    brand text NOT NULL,
    category_id text NOT NULL,
    list_price_eur numeric(12,2) NOT NULL,
    cost_price_eur numeric(12,2) NOT NULL,
    is_active boolean NOT NULL,
    launched_date date NOT NULL,
    _ingested_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (product_id)
);

CREATE INDEX IF NOT EXISTS products_ingested_at ON products (_ingested_at);

CREATE TABLE IF NOT EXISTS customers (
    customer_id text NOT NULL,
    email text,
    first_name text,
    last_name text,
    date_of_birth date,
    signup_ts timestamp NOT NULL,
    country_code text,
    city text,
    postal_code text,
    customer_segment text NOT NULL,
    marketing_opt_in boolean NOT NULL,
    is_deleted boolean NOT NULL,
    _ingested_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (customer_id)
);

CREATE INDEX IF NOT EXISTS customers_ingested_at ON customers (_ingested_at);

CREATE TABLE IF NOT EXISTS orders (
    order_id text NOT NULL,
    customer_id text NOT NULL,
    session_id text,
    order_ts timestamp NOT NULL,
    order_status text NOT NULL,
    updated_at timestamp NOT NULL,
    currency_code text NOT NULL,
    shipping_fee numeric(12,2) NOT NULL,
    voucher_code text,
    payment_method text NOT NULL,
    shipping_country text NOT NULL,
    is_test_order boolean NOT NULL,
    _ingested_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (order_id)
);

CREATE INDEX IF NOT EXISTS orders_ingested_at ON orders (_ingested_at);

CREATE TABLE IF NOT EXISTS order_items (
    order_date date NOT NULL,
    order_id text NOT NULL,
    line_number integer NOT NULL,
    product_id text NOT NULL,
    quantity integer NOT NULL,
    unit_price numeric(12,2) NOT NULL,
    discount_amount numeric(12,2) NOT NULL,
    tax_rate numeric(5,4) NOT NULL,
    _ingested_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (order_id, line_number)
);

CREATE INDEX IF NOT EXISTS order_items_ingested_at ON order_items (_ingested_at);

CREATE TABLE IF NOT EXISTS returns (
    return_id text NOT NULL,
    order_id text NOT NULL,
    line_number integer NOT NULL,
    product_id text NOT NULL,
    requested_ts timestamp NOT NULL,
    quantity_returned integer NOT NULL,
    refund_amount numeric(12,2),
    return_reason text NOT NULL,
    return_status text NOT NULL,
    updated_at timestamp NOT NULL,
    _ingested_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (return_id)
);

CREATE INDEX IF NOT EXISTS returns_ingested_at ON returns (_ingested_at);

CREATE TABLE IF NOT EXISTS _extract_watermark (
    dataset text PRIMARY KEY,
    high timestamp NOT NULL
);
