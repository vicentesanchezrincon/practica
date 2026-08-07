"""Generador de datos sinteticos para el sistema origen (Postgres).

Dos modos:

    --mode initial   Carga historica. Borra y repuebla las tablas.
    --mode daily     Simula un dia de actividad: altas nuevas, modificaciones
                     sobre filas existentes (que mueven updated_at) y un
                     porcentaje de datos sucios.

El modo `daily` es el que da sentido al pipeline:

  * los INSERT prueban la extraccion incremental,
  * los UPDATE prueban el MERGE de Silver y el SCD tipo 2 de Gold,
  * la suciedad prueba las reglas de calidad y la tabla de cuarentena.

Uso:
    make seed          # equivale a --mode initial
    make seed-daily    # equivale a --mode daily
"""

from __future__ import annotations

import argparse
import logging
import os
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import psycopg
from faker import Faker

# Los vocabularios del negocio vienen del pipeline, no de aqui. Silver valida
# contra estas mismas listas: con una copia local, anadir un estado nuevo al
# generador mandaria a cuarentena datos perfectamente buenos, y el sintoma
# aparecerian tres capas mas abajo.
from common.config import CATEGORIES, COUNTRIES, ORDER_STATUS, SEGMENTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
log = logging.getLogger("seed")


@dataclass(frozen=True)
class DirtRates:
    """Proporcion de filas a las que se les inyecta cada defecto.

    Todo esto es basura que la capa Silver tiene que detectar. Si subes estos
    valores por encima del umbral de calidad, la maquina de estados debe
    pararse antes de construir Gold: es la prueba 9 del plan.
    """

    null_required: float = 0.02  # email o customer_id a NULL
    bad_email: float = 0.03  # mayusculas y espacios sobrantes
    negative_amount: float = 0.01  # importes negativos
    duplicate_row: float = 0.02  # la misma fila insertada dos veces
    orphan_fk: float = 0.01  # referencia a un padre que no existe
    messy_country: float = 0.04  # ' es ', 'Es', 'ESP'...

    @classmethod
    def scaled(cls, factor: float) -> DirtRates:
        base = cls()
        return cls(
            null_required=base.null_required * factor,
            bad_email=base.bad_email * factor,
            negative_amount=base.negative_amount * factor,
            duplicate_row=base.duplicate_row * factor,
            orphan_fk=base.orphan_fk * factor,
            messy_country=base.messy_country * factor,
        )


# --------------------------------------------------------------- conexion ---


def connect() -> psycopg.Connection:
    """Conecta al Postgres origen usando las variables de entorno del compose."""
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "ecommerce"),
        user=os.getenv("POSTGRES_USER", "practica"),
        password=os.getenv("POSTGRES_PASSWORD", "practica_local_only"),
    )
    with conn.cursor() as cur:
        cur.execute("SET search_path TO ecommerce, public")
    conn.commit()
    return conn


# ----------------------------------------------------------- generadores ----


def dirty_email(fake: Faker, dirt: DirtRates) -> str | None:
    email = fake.unique.email()
    if random.random() < dirt.null_required:
        return None
    if random.random() < dirt.bad_email:
        # Lo que llega de un formulario sin validar
        return f"  {email.upper()} "
    return email


def dirty_country(dirt: DirtRates) -> str:
    code = random.choice(COUNTRIES)
    if random.random() < dirt.messy_country:
        return random.choice([code.lower(), f" {code} ", code.title(), code + "P"])
    return code


def make_customers(fake: Faker, n: int, dirt: DirtRates, since: datetime) -> list[tuple]:
    rows = []
    for _ in range(n):
        created = fake.date_time_between(start_date=since, tzinfo=UTC)
        row = (
            dirty_email(fake, dirt),
            fake.first_name(),
            fake.last_name(),
            dirty_country(dirt),
            fake.city(),
            random.choice(SEGMENTS),
            random.random() < 0.4,
            created,
            created,
        )
        rows.append(row)
        if random.random() < dirt.duplicate_row:
            rows.append(row)  # duplicado exacto: trabajo para el dedup de Silver
    return rows


def make_products(fake: Faker, n: int, dirt: DirtRates, since: datetime) -> list[tuple]:
    rows = []
    for i in range(n):
        created = fake.date_time_between(start_date=since, tzinfo=UTC)
        price = round(random.uniform(3.0, 900.0), 2)
        if random.random() < dirt.negative_amount:
            price = -price
        rows.append(
            (
                f"SKU-{i:06d}",
                fake.catch_phrase(),
                random.choice(CATEGORIES),
                price,
                random.random() < 0.9,
                created,
                created,
            )
        )
    return rows


def make_orders(
    fake: Faker,
    n: int,
    customer_ids: list[int],
    dirt: DirtRates,
    day_range: tuple[date, date],
) -> list[tuple]:
    rows = []
    start, end = day_range
    max_customer_id = max(customer_ids) if customer_ids else 0
    for _ in range(n):
        customer_id = random.choice(customer_ids)
        if random.random() < dirt.orphan_fk:
            customer_id = max_customer_id + random.randint(10_000, 99_999)  # huerfano
        elif random.random() < dirt.null_required:
            customer_id = None

        order_date = fake.date_between(start_date=start, end_date=end)
        created = datetime.combine(order_date, datetime.min.time(), tzinfo=UTC)
        total = round(random.uniform(5.0, 2_500.0), 2)
        if random.random() < dirt.negative_amount:
            total = -total

        row = (
            customer_id,
            order_date,
            random.choice(ORDER_STATUS),
            "EUR",
            total,
            created,
            created,
        )
        rows.append(row)
        if random.random() < dirt.duplicate_row:
            rows.append(row)
    return rows


def make_order_items(
    n_per_order: tuple[int, int],
    orders: list[tuple[int, datetime]],
    product_ids: list[int],
    dirt: DirtRates,
) -> list[tuple]:
    """Genera lineas de pedido.

    `orders` son pares (order_id, created_at). La linea hereda el timestamp del
    pedido padre: si le pusieramos now(), toda la tabla tendria el mismo
    updated_at y la primera extraccion incremental se traeria el historico
    entero en vez de solo lo que cambio ese dia.
    """
    rows = []
    max_order_id = max((oid for oid, _ in orders), default=0)
    lo, hi = n_per_order
    for order_id, created in orders:
        for _ in range(random.randint(lo, hi)):
            oid = order_id
            if random.random() < dirt.orphan_fk:
                oid = max_order_id + random.randint(10_000, 99_999)
            qty = random.randint(1, 5)
            price = round(random.uniform(3.0, 900.0), 2)
            if random.random() < dirt.negative_amount:
                qty = -qty
            rows.append(
                (
                    oid,
                    random.choice(product_ids),
                    qty,
                    price,
                    round(qty * price, 2),
                    created,
                    created,
                )
            )
    return rows


# ------------------------------------------------------------- escritura ----


def copy_rows(conn: psycopg.Connection, table: str, columns: list[str], rows: list[tuple]) -> None:
    """Carga masiva con COPY. Mucho mas rapido que un INSERT por fila."""
    if not rows:
        return
    cols = ", ".join(columns)
    with conn.cursor() as cur, cur.copy(f"COPY {table} ({cols}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
    conn.commit()
    log.info("%-12s  +%d filas", table, len(rows))


def fetch_ids(conn: psycopg.Connection, table: str, pk: str) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {pk} FROM {table}")  # noqa: S608
        return [r[0] for r in cur.fetchall()]


def max_id(conn: psycopg.Connection, table: str, pk: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT coalesce(max({pk}), 0) FROM {table}")  # noqa: S608
        return cur.fetchone()[0]


def fetch_orders(conn: psycopg.Connection, above: int = 0) -> list[tuple[int, datetime]]:
    """Pares (order_id, created_at). Con `above` se limita a los pedidos recien
    insertados, sin depender de un ORDER BY random()."""
    with conn.cursor() as cur:
        cur.execute("SELECT order_id, created_at FROM orders WHERE order_id > %s", (above,))
        return cur.fetchall()


# ----------------------------------------------------------------- modos ----

CUSTOMER_COLS = [
    "email", "first_name", "last_name", "country_code", "city",
    "segment", "marketing_opt_in", "created_at", "updated_at",
]  # fmt: skip
PRODUCT_COLS = ["sku", "name", "category", "unit_price", "is_active", "created_at", "updated_at"]
ORDER_COLS = ["customer_id", "order_date", "status", "currency", "total_amount", "created_at", "updated_at"]  # fmt: skip
ITEM_COLS = ["order_id", "product_id", "quantity", "unit_price", "line_amount", "created_at", "updated_at"]  # fmt: skip


def run_initial(conn: psycopg.Connection, fake: Faker, args: argparse.Namespace) -> None:
    dirt = DirtRates.scaled(args.dirt_factor)
    log.info("Carga inicial: %d clientes, %d productos, %d pedidos", args.customers, args.products, args.orders)  # fmt: skip

    with conn.cursor() as cur:
        cur.execute("TRUNCATE order_items, orders, products, customers RESTART IDENTITY CASCADE")
    conn.commit()

    since = datetime.now(UTC) - timedelta(days=args.history_days)

    copy_rows(conn, "customers", CUSTOMER_COLS, make_customers(fake, args.customers, dirt, since))
    copy_rows(conn, "products", PRODUCT_COLS, make_products(fake, args.products, dirt, since))

    customer_ids = fetch_ids(conn, "customers", "customer_id")
    product_ids = fetch_ids(conn, "products", "product_id")

    day_range = (since.date(), date.today() - timedelta(days=1))
    copy_rows(conn, "orders", ORDER_COLS, make_orders(fake, args.orders, customer_ids, dirt, day_range))  # fmt: skip

    orders = fetch_orders(conn)
    copy_rows(conn, "order_items", ITEM_COLS, make_order_items((1, 5), orders, product_ids, dirt))

    summarise(conn)


def run_daily(conn: psycopg.Connection, fake: Faker, args: argparse.Namespace) -> None:
    """Un dia de actividad: altas, modificaciones y suciedad."""
    dirt = DirtRates.scaled(args.dirt_factor)
    today = date.today()

    customer_ids = fetch_ids(conn, "customers", "customer_id")
    product_ids = fetch_ids(conn, "products", "product_id")
    if not customer_ids or not product_ids:
        raise SystemExit("No hay datos base. Ejecuta primero: make seed")

    # --- altas ---
    n_new_customers = max(1, args.customers // 50)
    n_new_orders = max(1, args.orders // 30)
    since = datetime.now(UTC) - timedelta(days=1)

    copy_rows(conn, "customers", CUSTOMER_COLS, make_customers(fake, n_new_customers, dirt, since))
    customer_ids = fetch_ids(conn, "customers", "customer_id")

    # Marcamos el corte ANTES de insertar para poder recuperar despues justo los
    # pedidos nuevos: sus lineas deben colgar de ellos, no de pedidos antiguos.
    last_order_id = max_id(conn, "orders", "order_id")
    copy_rows(conn, "orders", ORDER_COLS, make_orders(fake, n_new_orders, customer_ids, dirt, (today, today)))  # fmt: skip

    new_orders = fetch_orders(conn, above=last_order_id)
    copy_rows(conn, "order_items", ITEM_COLS, make_order_items((1, 4), new_orders, product_ids, dirt))  # fmt: skip

    # --- modificaciones: el trigger mueve updated_at, asi que el incremental las vera ---
    with conn.cursor() as cur:
        # Cambio de segmento -> esto es lo que genera versiones nuevas en el SCD2 de dim_customer
        cur.execute(
            """
            UPDATE customers SET segment = %s
            WHERE customer_id IN (
                SELECT customer_id FROM customers ORDER BY random() LIMIT %s
            )
            """,
            (random.choice(SEGMENTS), max(1, len(customer_ids) // 20)),
        )
        log.info("customers    ~%d filas modificadas (segmento)", cur.rowcount)

        # Avance del estado de pedidos recientes
        cur.execute(
            """
            UPDATE orders SET status = %s
            WHERE order_id IN (
                SELECT order_id FROM orders
                WHERE status IN ('pending', 'paid')
                ORDER BY random() LIMIT %s
            )
            """,
            (random.choice(["shipped", "delivered", "cancelled"]), max(1, n_new_orders * 2)),
        )
        log.info("orders       ~%d filas modificadas (estado)", cur.rowcount)
    conn.commit()

    summarise(conn)


def summarise(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        log.info("--- estado del origen ---")
        for table in ("customers", "products", "orders", "order_items"):
            cur.execute(f"SELECT count(*), max(updated_at) FROM {table}")  # noqa: S608
            count, last = cur.fetchone()
            log.info("%-12s  %8d filas   ultimo updated_at: %s", table, count, last)


# ------------------------------------------------------------------ main ----


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)  # fmt: skip
    p.add_argument("--mode", choices=["initial", "daily"], required=True)
    p.add_argument("--customers", type=int, default=20_000, help="clientes en la carga inicial")
    p.add_argument("--products", type=int, default=2_000, help="productos en la carga inicial")
    p.add_argument("--orders", type=int, default=80_000, help="pedidos en la carga inicial")
    p.add_argument("--history-days", type=int, default=365, help="profundidad del historico")
    p.add_argument(
        "--dirt-factor",
        type=float,
        default=1.0,
        help="multiplicador de datos sucios. 0 = datos limpios, 10 = lote corrupto "
        "(util para probar que el gate de calidad para el pipeline)",
    )
    p.add_argument("--seed", type=int, default=42, help="semilla para que sea reproducible")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)
    fake = Faker("es_ES")
    Faker.seed(args.seed)

    with connect() as conn:
        if args.mode == "initial":
            run_initial(conn, fake, args)
        else:
            run_daily(conn, fake, args)


if __name__ == "__main__":
    main()
