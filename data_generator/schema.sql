-- Sistema origen: e-commerce.
--
-- Detalle clave para el pipeline: TODAS las tablas llevan updated_at con
-- un trigger que lo refresca en cada UPDATE. Esa columna es la que permite
-- la extraccion incremental (CDC por watermark) en la capa Bronze.
--
-- Se aplica con:  make schema

CREATE SCHEMA IF NOT EXISTS ecommerce;
SET search_path TO ecommerce, public;

-- Para que una sesion interactiva (`make psql`) vea las tablas sin tener que
-- cualificarlas con ecommerce. cada vez.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET search_path TO ecommerce, public',
                   current_database());
END $$;

-- --------------------------------------------------------------- trigger ---

CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- --------------------------------------------------------------- tablas ----

CREATE TABLE IF NOT EXISTS customers (
    customer_id   BIGSERIAL PRIMARY KEY,
    email         TEXT,
    first_name    TEXT,
    last_name     TEXT,
    country_code  TEXT,
    city          TEXT,
    -- Estos dos cambian con el tiempo: son los que alimentan el SCD tipo 2 en Gold
    segment       TEXT,
    marketing_opt_in BOOLEAN DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    product_id    BIGSERIAL PRIMARY KEY,
    sku           TEXT,
    name          TEXT,
    category      TEXT,
    unit_price    NUMERIC(10, 2),
    is_active     BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id      BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT,
    order_date    DATE,
    status        TEXT,
    currency      TEXT DEFAULT 'EUR',
    total_amount  NUMERIC(12, 2),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id      BIGINT,
    product_id    BIGINT,
    quantity      INTEGER,
    unit_price    NUMERIC(10, 2),
    line_amount   NUMERIC(12, 2),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Deliberadamente NO declaramos FOREIGN KEY en orders/order_items.
-- Asi el generador puede inyectar huerfanos y la capa Silver tiene algo
-- real que detectar en la validacion de integridad referencial.

-- --------------------------------------------------------------- indices ---
-- Sin indice en updated_at, la extraccion incremental hace seq scan de la
-- tabla entera en cada ejecucion. Es exactamente el problema que veras en
-- produccion si nadie lo penso.

CREATE INDEX IF NOT EXISTS idx_customers_updated_at   ON customers (updated_at);
CREATE INDEX IF NOT EXISTS idx_products_updated_at    ON products (updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at      ON orders (updated_at);
CREATE INDEX IF NOT EXISTS idx_order_items_updated_at ON order_items (updated_at);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id     ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id   ON order_items (order_id);

-- -------------------------------------------------------------- triggers ---

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['customers', 'products', 'orders', 'order_items']
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%1$s_updated_at ON %1$s', t);
        EXECUTE format(
            'CREATE TRIGGER trg_%1$s_updated_at
             BEFORE UPDATE ON %1$s
             FOR EACH ROW EXECUTE FUNCTION touch_updated_at()', t);
    END LOOP;
END $$;
