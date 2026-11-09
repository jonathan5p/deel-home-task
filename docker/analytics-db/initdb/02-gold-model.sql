-- Safe to apply again to an existing database: every statement is idempotent.

BEGIN;

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id bigint NOT NULL,
    customer_name varchar,
    customer_address varchar,
    is_active boolean,
    _commit_sequence bigint NOT NULL,
    _valid_from timestamp NOT NULL,
    _valid_to timestamp,
    _is_current boolean NOT NULL,
    UNIQUE (customer_id, _commit_sequence),
    CHECK (_is_current = (_valid_to IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS dim_customer_current_idx
    ON gold.dim_customer (customer_id) WHERE _is_current;

COMMENT ON TABLE gold.dim_customer IS
    'Customers, SCD2: one row per customer version.';
COMMENT ON COLUMN gold.dim_customer.customer_key IS
    'Surrogate key of the customer version.';
COMMENT ON COLUMN gold.dim_customer.customer_id IS
    'Source customer id.';
COMMENT ON COLUMN gold.dim_customer.customer_name IS
    'Customer name in this version.';
COMMENT ON COLUMN gold.dim_customer.customer_address IS
    'Customer address in this version.';
COMMENT ON COLUMN gold.dim_customer.is_active IS
    'Whether the customer is active in the source in this version.';
COMMENT ON COLUMN gold.dim_customer._commit_sequence IS
    'Source WAL position (LSN) of the change that created this version.';
COMMENT ON COLUMN gold.dim_customer._valid_from IS
    'Source commit time from which this version is valid.';
COMMENT ON COLUMN gold.dim_customer._valid_to IS
    'Source commit time of the next version; null for the current version.';
COMMENT ON COLUMN gold.dim_customer._is_current IS
    'Whether this is the latest version of the customer.';

CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id bigint NOT NULL,
    product_name varchar,
    barcode varchar,
    -- The source price is a DECIMAL without precision or scale; an unconstrained
    -- numeric keeps every price exact.
    unit_price numeric,
    is_active boolean,
    _commit_sequence bigint NOT NULL,
    _valid_from timestamp NOT NULL,
    _valid_to timestamp,
    _is_current boolean NOT NULL,
    UNIQUE (product_id, _commit_sequence),
    CHECK (_is_current = (_valid_to IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS dim_product_current_idx
    ON gold.dim_product (product_id) WHERE _is_current;

COMMENT ON TABLE gold.dim_product IS
    'Products, SCD2: one row per product version; a new price is a new version.';
COMMENT ON COLUMN gold.dim_product.product_key IS
    'Surrogate key of the product version.';
COMMENT ON COLUMN gold.dim_product.product_id IS
    'Source product id.';
COMMENT ON COLUMN gold.dim_product.product_name IS
    'Product name in this version.';
COMMENT ON COLUMN gold.dim_product.barcode IS
    'Product barcode in this version.';
COMMENT ON COLUMN gold.dim_product.unit_price IS
    'Unit price in this version (source unity_price), exact.';
COMMENT ON COLUMN gold.dim_product.is_active IS
    'Whether the product is active in the source in this version.';
COMMENT ON COLUMN gold.dim_product._commit_sequence IS
    'Source WAL position (LSN) of the change that created this version.';
COMMENT ON COLUMN gold.dim_product._valid_from IS
    'Source commit time from which this version is valid.';
COMMENT ON COLUMN gold.dim_product._valid_to IS
    'Source commit time of the next version; null for the current version.';
COMMENT ON COLUMN gold.dim_product._is_current IS
    'Whether this is the latest version of the product.';

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key int PRIMARY KEY,
    calendar_date date NOT NULL UNIQUE,
    year int NOT NULL,
    quarter int NOT NULL,
    month int NOT NULL,
    day_of_month int NOT NULL,
    day_of_week int NOT NULL,
    is_weekend boolean NOT NULL
);

INSERT INTO gold.dim_date (
    date_key, calendar_date, year, quarter, month, day_of_month, day_of_week, is_weekend
)
SELECT
    to_char(day, 'YYYYMMDD')::int,
    day,
    extract(YEAR FROM day)::int,
    extract(QUARTER FROM day)::int,
    extract(MONTH FROM day)::int,
    extract(DAY FROM day)::int,
    extract(ISODOW FROM day)::int,
    extract(ISODOW FROM day) >= 6
FROM generate_series(date '2000-01-01', date '2099-12-31', interval '1 day') AS days (moment)
CROSS JOIN LATERAL (SELECT moment::date AS day) AS calendar
ON CONFLICT (date_key) DO NOTHING;

COMMENT ON TABLE gold.dim_date IS
    'Calendar days from 2000-01-01 to 2099-12-31, one row per day.';
COMMENT ON COLUMN gold.dim_date.date_key IS
    'Day as an integer yyyymmdd.';
COMMENT ON COLUMN gold.dim_date.calendar_date IS
    'The calendar day.';
COMMENT ON COLUMN gold.dim_date.year IS
    'Calendar year.';
COMMENT ON COLUMN gold.dim_date.quarter IS
    'Calendar quarter, 1 to 4.';
COMMENT ON COLUMN gold.dim_date.month IS
    'Calendar month, 1 to 12.';
COMMENT ON COLUMN gold.dim_date.day_of_month IS
    'Day of the month, 1 to 31.';
COMMENT ON COLUMN gold.dim_date.day_of_week IS
    'ISO day of the week, 1 (Monday) to 7 (Sunday).';
COMMENT ON COLUMN gold.dim_date.is_weekend IS
    'Whether the day is a Saturday or a Sunday.';

CREATE TABLE IF NOT EXISTS gold.dim_order_status (
    status_key smallint PRIMARY KEY,
    status_code varchar NOT NULL UNIQUE,
    is_open boolean NOT NULL
);

INSERT INTO gold.dim_order_status (status_key, status_code, is_open)
VALUES
    (1, 'PENDING', true),
    (2, 'PROCESSING', true),
    (3, 'REPROCESSING', true),
    (4, 'COMPLETED', false),
    (5, 'UNKNOWN', true)
ON CONFLICT (status_key) DO NOTHING;

COMMENT ON TABLE gold.dim_order_status IS
    'Order statuses; a null or unexpected source status maps to UNKNOWN.';
COMMENT ON COLUMN gold.dim_order_status.status_key IS
    'Stable key of the status.';
COMMENT ON COLUMN gold.dim_order_status.status_code IS
    'Status as written in the source, or UNKNOWN.';
COMMENT ON COLUMN gold.dim_order_status.is_open IS
    'Whether an order in this status is open; every status except COMPLETED is.';

-- The partition column must be part of the primary key; a change always has the same
-- _changed_at, so the key still identifies one change.
CREATE TABLE IF NOT EXISTS gold.fact_order_history (
    order_id bigint NOT NULL,
    _commit_sequence bigint NOT NULL,
    _changed_at timestamp NOT NULL,
    _change_op varchar NOT NULL CHECK (_change_op IN ('snapshot', 'create', 'update', 'delete')),
    status_key smallint NOT NULL REFERENCES gold.dim_order_status (status_key),
    customer_id bigint,
    customer_key bigint REFERENCES gold.dim_customer (customer_key),
    order_date_key int REFERENCES gold.dim_date (date_key),
    delivery_date_key int REFERENCES gold.dim_date (date_key),
    PRIMARY KEY (order_id, _commit_sequence, _changed_at)
) PARTITION BY RANGE (_changed_at);

CREATE TABLE IF NOT EXISTS gold.fact_order_history_default
    PARTITION OF gold.fact_order_history DEFAULT;

COMMENT ON TABLE gold.fact_order_history IS
    'Order history: one row per change to an order, partitioned by month of _changed_at.';
COMMENT ON TABLE gold.fact_order_history_default IS
    'Order history rows outside every monthly partition.';
COMMENT ON COLUMN gold.fact_order_history.order_id IS
    'Source order id.';
COMMENT ON COLUMN gold.fact_order_history._commit_sequence IS
    'Source WAL position (LSN) of the change; orders the versions of an order.';
COMMENT ON COLUMN gold.fact_order_history._changed_at IS
    'When the change was committed in the source.';
COMMENT ON COLUMN gold.fact_order_history._change_op IS
    'Kind of change: snapshot, create, update or delete.';
COMMENT ON COLUMN gold.fact_order_history.status_key IS
    'Order status after the change.';
COMMENT ON COLUMN gold.fact_order_history.customer_id IS
    'Source customer id of the order.';
COMMENT ON COLUMN gold.fact_order_history.customer_key IS
    'Customer version valid at _changed_at; null if the customer is not known yet.';
COMMENT ON COLUMN gold.fact_order_history.order_date_key IS
    'Order date as a dim_date key.';
COMMENT ON COLUMN gold.fact_order_history.delivery_date_key IS
    'Delivery date as a dim_date key.';

CREATE TABLE IF NOT EXISTS gold.fact_order_item_history (
    order_item_id bigint NOT NULL,
    _commit_sequence bigint NOT NULL,
    _changed_at timestamp NOT NULL,
    _change_op varchar NOT NULL CHECK (_change_op IN ('snapshot', 'create', 'update', 'delete')),
    order_id bigint,
    product_id bigint,
    product_key bigint REFERENCES gold.dim_product (product_key),
    quantity int,
    unit_price numeric,
    line_amount numeric,
    PRIMARY KEY (order_item_id, _commit_sequence, _changed_at)
) PARTITION BY RANGE (_changed_at);

CREATE TABLE IF NOT EXISTS gold.fact_order_item_history_default
    PARTITION OF gold.fact_order_item_history DEFAULT;

COMMENT ON TABLE gold.fact_order_item_history IS
    'Order item history: one row per change to an order item, partitioned by month of _changed_at.';
COMMENT ON TABLE gold.fact_order_item_history_default IS
    'Order item history rows outside every monthly partition.';
COMMENT ON COLUMN gold.fact_order_item_history.order_item_id IS
    'Source order item id.';
COMMENT ON COLUMN gold.fact_order_item_history._commit_sequence IS
    'Source WAL position (LSN) of the change; orders the versions of an item.';
COMMENT ON COLUMN gold.fact_order_item_history._changed_at IS
    'When the change was committed in the source.';
COMMENT ON COLUMN gold.fact_order_item_history._change_op IS
    'Kind of change: snapshot, create, update or delete.';
COMMENT ON COLUMN gold.fact_order_item_history.order_id IS
    'Source order id of the item.';
COMMENT ON COLUMN gold.fact_order_item_history.product_id IS
    'Source product id of the item.';
COMMENT ON COLUMN gold.fact_order_item_history.product_key IS
    'Product version valid at _changed_at; null if the product is not known yet.';
COMMENT ON COLUMN gold.fact_order_item_history.quantity IS
    'Ordered quantity (source quanity).';
COMMENT ON COLUMN gold.fact_order_item_history.unit_price IS
    'Unit price of the product version valid at _changed_at.';
COMMENT ON COLUMN gold.fact_order_item_history.line_amount IS
    'quantity × unit_price.';

-- Rows the default partition already holds for a new month are moved into it, because a
-- partition cannot be added while the default partition holds rows in its range.
CREATE OR REPLACE FUNCTION gold.ensure_history_partitions(months_ahead int DEFAULT 2)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    fact text;
    month_start date;
    month_end date;
    partition_name text;
BEGIN
    FOREACH fact IN ARRAY ARRAY['fact_order_history', 'fact_order_item_history'] LOOP
        FOR offset_months IN 0..months_ahead LOOP
            month_start := date_trunc('month', localtimestamp)::date
                + make_interval(months => offset_months);
            month_end := month_start + interval '1 month';
            partition_name := format('%s_%s', fact, to_char(month_start, 'YYYY_MM'));
            CONTINUE WHEN to_regclass(format('gold.%I', partition_name)) IS NOT NULL;

            EXECUTE format(
                'CREATE TABLE gold.%I (LIKE gold.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS)',
                partition_name, fact
            );
            EXECUTE format(
                'WITH moved AS (DELETE FROM gold.%I WHERE _changed_at >= $1 AND _changed_at < $2'
                ' RETURNING *) INSERT INTO gold.%I SELECT * FROM moved',
                fact || '_default', partition_name
            ) USING month_start, month_end;
            EXECUTE format(
                'ALTER TABLE gold.%I ATTACH PARTITION gold.%I FOR VALUES FROM (%L) TO (%L)',
                fact, partition_name, month_start, month_end
            );
            EXECUTE format(
                'COMMENT ON TABLE gold.%I IS %L',
                partition_name,
                format('Partition of %s for %s.', fact, to_char(month_start, 'YYYY-MM'))
            );
        END LOOP;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION gold.ensure_history_partitions(int) IS
    'Creates the monthly history partitions from the current month to months_ahead months ahead.';

SELECT gold.ensure_history_partitions(2);

CREATE OR REPLACE VIEW gold.fact_order_history_versions AS
SELECT
    order_id,
    _commit_sequence,
    _changed_at,
    _change_op,
    status_key,
    customer_id,
    customer_key,
    order_date_key,
    delivery_date_key,
    lead(_changed_at) OVER versions AS _valid_to,
    lead(_changed_at) OVER versions IS NULL AS _is_current
FROM gold.fact_order_history
WINDOW versions AS (PARTITION BY order_id ORDER BY _commit_sequence);

COMMENT ON VIEW gold.fact_order_history_versions IS
    'Order history with the validity range of each version.';
COMMENT ON COLUMN gold.fact_order_history_versions.order_id IS
    'Source order id.';
COMMENT ON COLUMN gold.fact_order_history_versions._commit_sequence IS
    'Source WAL position (LSN) of the change; orders the versions of an order.';
COMMENT ON COLUMN gold.fact_order_history_versions._changed_at IS
    'When the change was committed in the source; the version is valid from then.';
COMMENT ON COLUMN gold.fact_order_history_versions._change_op IS
    'Kind of change: snapshot, create, update or delete.';
COMMENT ON COLUMN gold.fact_order_history_versions.status_key IS
    'Order status after the change.';
COMMENT ON COLUMN gold.fact_order_history_versions.customer_id IS
    'Source customer id of the order.';
COMMENT ON COLUMN gold.fact_order_history_versions.customer_key IS
    'Customer version valid at _changed_at.';
COMMENT ON COLUMN gold.fact_order_history_versions.order_date_key IS
    'Order date as a dim_date key.';
COMMENT ON COLUMN gold.fact_order_history_versions.delivery_date_key IS
    'Delivery date as a dim_date key.';
COMMENT ON COLUMN gold.fact_order_history_versions._valid_to IS
    '_changed_at of the next version by _commit_sequence; null for the latest version.';
COMMENT ON COLUMN gold.fact_order_history_versions._is_current IS
    'Whether this is the latest version of the order, a delete included.';

CREATE OR REPLACE VIEW gold.fact_order_item_history_versions AS
SELECT
    order_item_id,
    _commit_sequence,
    _changed_at,
    _change_op,
    order_id,
    product_id,
    product_key,
    quantity,
    unit_price,
    line_amount,
    lead(_changed_at) OVER versions AS _valid_to,
    lead(_changed_at) OVER versions IS NULL AS _is_current
FROM gold.fact_order_item_history
WINDOW versions AS (PARTITION BY order_item_id ORDER BY _commit_sequence);

COMMENT ON VIEW gold.fact_order_item_history_versions IS
    'Order item history with the validity range of each version.';
COMMENT ON COLUMN gold.fact_order_item_history_versions.order_item_id IS
    'Source order item id.';
COMMENT ON COLUMN gold.fact_order_item_history_versions._commit_sequence IS
    'Source WAL position (LSN) of the change; orders the versions of an item.';
COMMENT ON COLUMN gold.fact_order_item_history_versions._changed_at IS
    'When the change was committed in the source; the version is valid from then.';
COMMENT ON COLUMN gold.fact_order_item_history_versions._change_op IS
    'Kind of change: snapshot, create, update or delete.';
COMMENT ON COLUMN gold.fact_order_item_history_versions.order_id IS
    'Source order id of the item.';
COMMENT ON COLUMN gold.fact_order_item_history_versions.product_id IS
    'Source product id of the item.';
COMMENT ON COLUMN gold.fact_order_item_history_versions.product_key IS
    'Product version valid at _changed_at.';
COMMENT ON COLUMN gold.fact_order_item_history_versions.quantity IS
    'Ordered quantity.';
COMMENT ON COLUMN gold.fact_order_item_history_versions.unit_price IS
    'Unit price of the product version valid at _changed_at.';
COMMENT ON COLUMN gold.fact_order_item_history_versions.line_amount IS
    'quantity × unit_price.';
COMMENT ON COLUMN gold.fact_order_item_history_versions._valid_to IS
    '_changed_at of the next version by _commit_sequence; null for the latest version.';
COMMENT ON COLUMN gold.fact_order_item_history_versions._is_current IS
    'Whether this is the latest version of the item, a delete included.';

CREATE TABLE IF NOT EXISTS gold.current_orders (
    order_id bigint PRIMARY KEY,
    status_key smallint NOT NULL REFERENCES gold.dim_order_status (status_key),
    customer_id bigint,
    order_date date,
    delivery_date date,
    order_date_key int REFERENCES gold.dim_date (date_key),
    delivery_date_key int REFERENCES gold.dim_date (date_key),
    _commit_sequence bigint NOT NULL,
    _changed_at timestamp NOT NULL,
    _is_deleted boolean NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS current_orders_status_delivery_idx
    ON gold.current_orders (status_key, delivery_date);
CREATE INDEX IF NOT EXISTS current_orders_customer_idx
    ON gold.current_orders (customer_id);

COMMENT ON TABLE gold.current_orders IS
    'Latest version of every order, deleted ones included; read by the business questions.';
COMMENT ON COLUMN gold.current_orders.order_id IS
    'Source order id.';
COMMENT ON COLUMN gold.current_orders.status_key IS
    'Current order status.';
COMMENT ON COLUMN gold.current_orders.customer_id IS
    'Source customer id; joins to the current dim_customer row.';
COMMENT ON COLUMN gold.current_orders.order_date IS
    'Date the order was placed.';
COMMENT ON COLUMN gold.current_orders.delivery_date IS
    'Planned delivery date.';
COMMENT ON COLUMN gold.current_orders.order_date_key IS
    'Order date as a dim_date key.';
COMMENT ON COLUMN gold.current_orders.delivery_date_key IS
    'Delivery date as a dim_date key.';
COMMENT ON COLUMN gold.current_orders._commit_sequence IS
    'Source WAL position (LSN) of the stored version; only a higher one replaces it.';
COMMENT ON COLUMN gold.current_orders._changed_at IS
    'When the stored version was committed in the source.';
COMMENT ON COLUMN gold.current_orders._is_deleted IS
    'Whether the order was deleted in the source.';

-- No foreign key to current_orders: an item can arrive before its order.
CREATE TABLE IF NOT EXISTS gold.current_order_items (
    order_item_id bigint PRIMARY KEY,
    order_id bigint,
    product_id bigint,
    quantity int,
    _commit_sequence bigint NOT NULL,
    _changed_at timestamp NOT NULL,
    _is_deleted boolean NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS current_order_items_order_idx
    ON gold.current_order_items (order_id);
CREATE INDEX IF NOT EXISTS current_order_items_product_idx
    ON gold.current_order_items (product_id);

COMMENT ON TABLE gold.current_order_items IS
    'Latest version of every order item, deleted ones included; read by the business questions.';
COMMENT ON COLUMN gold.current_order_items.order_item_id IS
    'Source order item id.';
COMMENT ON COLUMN gold.current_order_items.order_id IS
    'Source order id; joins to current_orders.';
COMMENT ON COLUMN gold.current_order_items.product_id IS
    'Source product id; joins to the current dim_product row.';
COMMENT ON COLUMN gold.current_order_items.quantity IS
    'Ordered quantity.';
COMMENT ON COLUMN gold.current_order_items._commit_sequence IS
    'Source WAL position (LSN) of the stored version; only a higher one replaces it.';
COMMENT ON COLUMN gold.current_order_items._changed_at IS
    'When the stored version was committed in the source.';
COMMENT ON COLUMN gold.current_order_items._is_deleted IS
    'Whether the item was deleted in the source.';

-- Temporary, deliberately simple proxy for freshness, to be replaced by real pipeline metrics:
-- the newest source change gold holds per entity. When the source is idle it ages even though
-- the pipeline is healthy.
CREATE OR REPLACE VIEW gold.data_freshness AS
SELECT 'customers' AS entity, max(_valid_from) AS last_changed_at FROM gold.dim_customer
UNION ALL
SELECT 'products', max(_valid_from) FROM gold.dim_product
UNION ALL
SELECT 'orders', max(_changed_at) FROM gold.current_orders
UNION ALL
SELECT 'order_items', max(_changed_at) FROM gold.current_order_items;

COMMENT ON VIEW gold.data_freshness IS
    'TEMPORARY proxy for freshness, to be replaced: newest source change time gold holds for each entity.';
COMMENT ON COLUMN gold.data_freshness.entity IS
    'Source entity: customers, products, orders or order_items.';
COMMENT ON COLUMN gold.data_freshness.last_changed_at IS
    'Source commit time of the newest change of the entity stored in gold; null while the entity is empty.';

COMMIT;
