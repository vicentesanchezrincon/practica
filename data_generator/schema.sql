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


-- ===========================================================================
-- Serie temporal: eventos de la tienda web
-- ===========================================================================
--
-- Es una fuente de otra naturaleza, y por eso vive en su propio esquema. Un
-- historian (PI System, InfluxDB, TimescaleDB, Timestream) guarda una medida
-- por sensor y por instante, para siempre, y NO la actualiza jamas. El flujo
-- de eventos de una web tiene exactamente esa forma.
--
-- Lo que rompe respecto a las cuatro tablas de arriba:
--
--   * no hay updated_at, porque una fila nunca se modifica;
--   * no hay clave secuencial, el id es un UUID que genera el cliente;
--   * hay DOS tiempos, y elegir el equivocado es el bug de esta capa;
--   * el customer_id nulo de un visitante anonimo es un dato CORRECTO.

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.web_events (
    -- Lo genera el cliente, no la base de datos. Es lo que permite reconocer
    -- un reenvio: la entrega es "al menos una vez", asi que el mismo evento
    -- llega repetido con relativa frecuencia.
    event_id     UUID        NOT NULL,

    -- Cuando OCURRIO el evento, segun el reloj del dispositivo.
    event_time   TIMESTAMPTZ NOT NULL,
    -- Cuando LLEGO al servidor. Un movil sin cobertura sincroniza horas
    -- despues, asi que received_at puede ir muy por detras de event_time.
    received_at  TIMESTAMPTZ NOT NULL,

    session_id   TEXT        NOT NULL,
    -- NULL en visitantes anonimos, y es correcto: la mayoria del trafico de
    -- una tienda no ha iniciado sesion. Aqui NO va un NOT NULL.
    customer_id  BIGINT,

    event_type   TEXT        NOT NULL,
    product_id   BIGINT,
    quantity     INTEGER,
    amount       NUMERIC(12, 2),
    device       TEXT,
    utm_source   TEXT,

    -- Carga semiestructurada: cada version de la web manda campos distintos.
    -- Es la deriva de esquema en su habitat natural.
    properties   JSONB
);

-- Sin PRIMARY KEY sobre event_id, y no es un olvido. Con una clave unica, el
-- reenvio de un evento fallaria en el INSERT y el problema quedaria resuelto
-- en el origen... y sin practicar. En un almacen de eventos real tampoco la
-- hay: se ingiere todo y se deduplica aguas abajo, porque rechazar en la
-- entrada significa perder el evento si el duplicado era el bueno.

-- --------------------------------------------------------- particionado ---
-- Aqui es donde el entorno local y AWS divergen, a proposito y de forma
-- controlada.
--
-- RDS PostgreSQL **no ofrece la extension timescaledb** en ninguna version, ni
-- Aurora tampoco (comprobado: no aparece en los AllowedValues de
-- shared_preload_libraries de postgres13 a postgres17). No es cuestion de
-- edicion ni de licencia: no esta.
--
-- Asi que el DDL detecta el motor y elige camino. La tabla se llama igual,
-- tiene las mismas columnas, y **el codigo de aplicacion no se entera**: Bronze
-- la lee por JDBC sin saber cual de los dos caminos se tomo. Lo unico que
-- cambia es como Postgres coloca las filas en disco.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb') THEN
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        -- Una hypertable es una tabla normal que por dentro se trocea en
        -- "chunks" por rango de tiempo. Se consulta como si fuera una sola.
        PERFORM create_hypertable(
            'analytics.web_events', 'event_time',
            chunk_time_interval => INTERVAL '1 day',
            if_not_exists       => TRUE,
            migrate_data        => TRUE
        );
        RAISE NOTICE 'web_events es una hypertable (chunks de 1 dia)';
    ELSE
        RAISE NOTICE 'sin timescaledb: web_events queda como tabla plana con indices BRIN';
    END IF;
END $$;

-- BRIN y no btree, en los dos caminos. Un indice BRIN guarda el rango de
-- valores de cada bloque de disco en vez de una entrada por fila: ocupa
-- ordenes de magnitud menos y funciona muy bien cuando los datos llegan ya
-- ordenados en el tiempo, que es justo el caso de un log de eventos.
--
-- Un btree sobre received_at en una tabla de millones de filas que solo crece
-- es de los indices mas caros de mantener y de los menos utiles.
CREATE INDEX IF NOT EXISTS idx_web_events_event_time
    ON analytics.web_events USING brin (event_time);
-- received_at es la columna del incremental: la extraccion pregunta siempre
-- "¿que ha llegado desde la ultima vez?".
CREATE INDEX IF NOT EXISTS idx_web_events_received_at
    ON analytics.web_events USING brin (received_at);
