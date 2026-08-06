"""Tests del MERGE de Silver sobre Iceberg.

Estos ejercitan las funciones reales del job (`ensure_table` y `merge`) contra
una tabla Iceberg de verdad en un warehouse temporal. No es un mock: se crea la
tabla, se hace el MERGE y se leen los resultados.

Es el test mas valioso de la fase. La idempotencia de todo el pipeline depende
de que este MERGE este bien: si duplica filas, cada reejecucion infla los datos
y nadie se entera hasta que los numeros de negocio dejan de cuadrar.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="Ejecuta los tests dentro del contenedor Glue: make test")

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "jobs"))

from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.config import get_table  # noqa: E402
from common.spark_session import CATALOG  # noqa: E402

DB = "silver_test"

CUSTOMERS = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("email", StringType()),
        StructField("segment", StringType()),
        StructField("updated_at", TimestampType()),
    ]
)
ORDERS = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("order_date", TimestampType()),
        StructField("total_amount", DoubleType()),
        StructField("updated_at", TimestampType()),
    ]
)


def t(dia: int) -> datetime:
    return datetime(2026, 3, dia)


@pytest.fixture(scope="module")
def silver():
    from silver_transform import ensure_table, merge

    return ensure_table, merge


@pytest.fixture
def tabla(spark, request):
    """Una tabla Iceberg limpia por test."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DB}")
    nombre = f"{CATALOG}.{DB}.{request.node.name[:40].replace('[', '_').replace(']', '')}"
    spark.sql(f"DROP TABLE IF EXISTS {nombre}")
    yield nombre
    spark.sql(f"DROP TABLE IF EXISTS {nombre}")


def cargar(spark, silver, tabla, filas, spec, schema):
    ensure_table, merge = silver
    df = spark.createDataFrame(filas, schema)
    ensure_table(spark, df, spec, tabla)
    merge(spark, df, spec, tabla)
    return spark.table(tabla)


# ------------------------------------------------------------ creacion ------


def test_la_tabla_se_crea_a_partir_del_dataframe(spark, silver, tabla):
    """El esquema no se declara en ningun sitio: sale del origen. Una fuente de
    verdad, no dos."""
    resultado = cargar(
        spark, silver, tabla, [(1, "a@b.com", "gold", t(1))], get_table("customers"), CUSTOMERS
    )
    assert set(resultado.columns) == {"customer_id", "email", "segment", "updated_at"}
    assert resultado.count() == 1


def test_la_tabla_es_iceberg(spark, silver, tabla):
    cargar(spark, silver, tabla, [(1, "a@b.com", "gold", t(1))], get_table("customers"), CUSTOMERS)
    # Solo una tabla Iceberg tiene la metatabla `.snapshots`.
    assert spark.table(f"{tabla}.snapshots").count() >= 1


def test_se_aplica_la_particion_declarada(spark, silver, tabla):
    """orders lleva silver_partition='months(order_date)'. La particion oculta
    de Iceberg permite filtrar por order_date sin conocer la particion."""
    cargar(spark, silver, tabla, [(1, 10, t(1), 50.0, t(1))], get_table("orders"), ORDERS)
    campos = spark.sql(f"SELECT * FROM {tabla}.partitions").columns
    assert any("partition" in c for c in campos)


# --------------------------------------------------------- idempotencia -----


def test_cargar_dos_veces_el_mismo_lote_no_duplica(spark, silver, tabla):
    """LA prueba de la fase. Si esto falla, cada reejecucion infla los datos."""
    ensure_table, merge = silver
    spec = get_table("customers")
    filas = [(1, "a@b.com", "gold", t(1)), (2, "c@d.com", "bronze", t(1))]

    df = spark.createDataFrame(filas, CUSTOMERS)
    ensure_table(spark, df, spec, tabla)

    merge(spark, df, spec, tabla)
    primera = spark.table(tabla).count()

    merge(spark, df, spec, tabla)
    segunda = spark.table(tabla).count()

    assert primera == segunda == 2


def test_el_contenido_tampoco_cambia_al_repetir(spark, silver, tabla):
    ensure_table, merge = silver
    spec = get_table("customers")
    df = spark.createDataFrame([(1, "a@b.com", "gold", t(1))], CUSTOMERS)

    ensure_table(spark, df, spec, tabla)
    merge(spark, df, spec, tabla)
    antes = spark.table(tabla).collect()

    merge(spark, df, spec, tabla)
    despues = spark.table(tabla).collect()

    assert antes == despues


# ----------------------------------------------------------- upsert ---------


def test_una_fila_modificada_se_actualiza_no_se_duplica(spark, silver, tabla):
    ensure_table, merge = silver
    spec = get_table("customers")

    inicial = spark.createDataFrame([(1, "viejo@b.com", "bronze", t(1))], CUSTOMERS)
    ensure_table(spark, inicial, spec, tabla)
    merge(spark, inicial, spec, tabla)

    cambio = spark.createDataFrame([(1, "nuevo@b.com", "gold", t(5))], CUSTOMERS)
    merge(spark, cambio, spec, tabla)

    filas = spark.table(tabla).collect()
    assert len(filas) == 1
    assert filas[0]["email"] == "nuevo@b.com"
    assert filas[0]["segment"] == "gold"


def test_una_fila_nueva_se_inserta(spark, silver, tabla):
    ensure_table, merge = silver
    spec = get_table("customers")

    inicial = spark.createDataFrame([(1, "a@b.com", "gold", t(1))], CUSTOMERS)
    ensure_table(spark, inicial, spec, tabla)
    merge(spark, inicial, spec, tabla)

    nueva = spark.createDataFrame([(2, "c@d.com", "bronze", t(2))], CUSTOMERS)
    merge(spark, nueva, spec, tabla)

    assert spark.table(tabla).count() == 2


def test_un_lote_mixto_actualiza_e_inserta_a_la_vez(spark, silver, tabla):
    """El caso real de cada dia: llegan altas y modificaciones mezcladas."""
    ensure_table, merge = silver
    spec = get_table("customers")

    inicial = spark.createDataFrame(
        [(1, "uno@b.com", "bronze", t(1)), (2, "dos@b.com", "bronze", t(1))], CUSTOMERS
    )
    ensure_table(spark, inicial, spec, tabla)
    merge(spark, inicial, spec, tabla)

    lote = spark.createDataFrame(
        [(2, "dos-nuevo@b.com", "gold", t(5)), (3, "tres@b.com", "silver", t(5))], CUSTOMERS
    )
    merge(spark, lote, spec, tabla)

    resultado = {f["customer_id"]: f["email"] for f in spark.table(tabla).collect()}
    assert resultado == {
        1: "uno@b.com",
        2: "dos-nuevo@b.com",
        3: "tres@b.com",
    }


def test_no_se_toca_lo_que_no_viene_en_el_lote(spark, silver, tabla):
    """Un incremental solo trae lo que cambio. Lo que no viene debe quedarse
    exactamente como estaba, no borrarse."""
    ensure_table, merge = silver
    spec = get_table("customers")

    inicial = spark.createDataFrame(
        [(i, f"c{i}@b.com", "bronze", t(1)) for i in range(1, 6)], CUSTOMERS
    )
    ensure_table(spark, inicial, spec, tabla)
    merge(spark, inicial, spec, tabla)

    merge(
        spark, spark.createDataFrame([(3, "c3-nuevo@b.com", "gold", t(9))], CUSTOMERS), spec, tabla
    )

    filas = {f["customer_id"]: f["email"] for f in spark.table(tabla).collect()}
    assert len(filas) == 5
    assert filas[3] == "c3-nuevo@b.com"
    assert filas[1] == "c1@b.com"


# ------------------------------------------------------- time travel -------


def test_iceberg_permite_volver_a_un_estado_anterior(spark, silver, tabla):
    """Time travel: poder responder "¿que decia esta tabla ayer?" es lo que
    convierte una auditoria de tres dias en una consulta."""
    ensure_table, merge = silver
    spec = get_table("customers")

    inicial = spark.createDataFrame([(1, "viejo@b.com", "bronze", t(1))], CUSTOMERS)
    ensure_table(spark, inicial, spec, tabla)
    merge(spark, inicial, spec, tabla)

    snapshot = spark.sql(
        f"SELECT snapshot_id FROM {tabla}.snapshots ORDER BY committed_at"
    ).collect()[-1][0]

    merge(spark, spark.createDataFrame([(1, "nuevo@b.com", "gold", t(5))], CUSTOMERS), spec, tabla)

    actual = spark.table(tabla).collect()[0]["email"]
    anterior = (
        spark.read.option("snapshot-id", snapshot)
        .format("iceberg")
        .load(tabla)
        .collect()[0]["email"]
    )

    assert actual == "nuevo@b.com"
    assert anterior == "viejo@b.com"
