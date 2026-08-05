"""Tests de la logica de la capa Bronze.

Se prueban las dos piezas que se pueden ejercitar sin AWS: las columnas de
linaje y la construccion de las consultas incrementales. El resto del job
(JDBC contra el RDS) solo se puede probar desplegado.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="Ejecuta los tests dentro del contenedor Glue: make test")

# Los jobs viven fuera del paquete `common`, asi que hay que ponerlos en el path.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "jobs"))

from common.config import SOURCE_SYSTEM, get_table  # noqa: E402
from common.watermark import EPOCH, format  # noqa: E402

MOMENTO = datetime(2026, 3, 15, 14, 30, 45, tzinfo=UTC)


@pytest.fixture(scope="module")
def add_lineage():
    from bronze_ingest import add_lineage as fn

    return fn


# ------------------------------------------------------------- linaje ------


def test_el_linaje_anade_las_cuatro_columnas(spark, add_lineage):
    df = spark.createDataFrame([(1, "a")], ["order_id", "status"])
    resultado = add_lineage(df, "lote-123", MOMENTO)

    for columna in ("_ingested_at", "_source_system", "_batch_id", "ingestion_date"):
        assert columna in resultado.columns


def test_el_linaje_no_toca_los_datos_originales(spark, add_lineage):
    """Bronze es una copia fiel: solo se anade, nunca se modifica."""
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["order_id", "status"])
    resultado = add_lineage(df, "lote-123", MOMENTO)

    assert resultado.count() == df.count()
    originales = resultado.select("order_id", "status").collect()
    assert [(r["order_id"], r["status"]) for r in originales] == [(1, "a"), (2, "b")]


def test_el_batch_id_permite_rastrear_la_fila_hasta_su_ejecucion(spark, add_lineage):
    df = spark.createDataFrame([(1,)], ["order_id"])
    fila = add_lineage(df, "jr_abc123", MOMENTO).collect()[0]

    assert fila["_batch_id"] == "jr_abc123"
    assert fila["_source_system"] == SOURCE_SYSTEM


def test_la_particion_es_la_fecha_de_ingesta_no_la_del_dato(spark, add_lineage):
    """Bronze se particiona por CUANDO se ingesto, no por cuando ocurrio el
    hecho. Asi una reingesta de datos antiguos cae en la particion de hoy y el
    historico previo queda intacto."""
    df = spark.createDataFrame([(1,)], ["order_id"])
    fila = add_lineage(df, "lote", MOMENTO).collect()[0]
    assert fila["ingestion_date"] == "2026-03-15"


def test_todas_las_filas_del_lote_comparten_marca_temporal(spark, add_lineage):
    """Si cada fila llevara su propio now(), un lote podria quedar repartido
    entre dos particiones al cruzar la medianoche."""
    df = spark.createDataFrame([(i,) for i in range(50)], ["order_id"])
    fechas = {r["ingestion_date"] for r in add_lineage(df, "lote", MOMENTO).collect()}
    assert len(fechas) == 1


# --------------------------------------------------- consulta incremental ---


def test_el_watermark_epoch_se_formatea_como_timestamp_valido():
    """La primera ejecucion mete EPOCH en un `WHERE updated_at > TIMESTAMP '...'`.
    Si el formato no fuera valido, el job fallaria justo en la carga inicial."""
    texto = format(EPOCH)
    assert texto.startswith("1970-01-01")
    assert "+00:00" in texto


@pytest.mark.parametrize("tabla", ["customers", "products", "orders", "order_items"])
def test_toda_tabla_ingestable_declara_watermark_y_particion(tabla):
    """Sin columna de particion, Spark lee la tabla con un solo hilo; sin
    columna de watermark no hay incremental posible."""
    spec = get_table(tabla)
    assert spec.watermark_column
    assert spec.partition_column, f"{tabla} no puede paralelizar la lectura JDBC"
