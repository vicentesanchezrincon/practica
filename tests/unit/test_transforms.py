"""Tests de normalizacion y deduplicacion.

Toda esta logica corre con Spark en local, sin AWS, asi que se puede probar
entera. Es la parte del pipeline donde un fallo no se nota: no da error, solo
produce datos ligeramente equivocados.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

import pytest

pytest.importorskip("pyspark", reason="Ejecuta los tests dentro del contenedor Glue: make test")

from pyspark.sql.types import (  # noqa: E402
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.config import get_table  # noqa: E402
from common.transforms import (  # noqa: E402
    deduplicate,
    drop_bronze_only_columns,
    normalize,
)

# Esquema explicito y no inferido: Spark no puede deducir el tipo de una columna
# que solo contiene nulos, y varios de estos tests prueban justo ese caso.
CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("email", StringType()),
        StructField("country_code", StringType()),
        StructField("segment", StringType()),
        StructField("updated_at", TimestampType()),
        StructField("_ingested_at", TimestampType()),
    ]
)


def customers_df(spark, filas):
    return spark.createDataFrame(filas, CUSTOMERS_SCHEMA)


def t(dia: int, hora: int = 0) -> datetime:
    return datetime(2026, 3, dia, hora, 0, 0)


# --------------------------------------------------------- normalizacion ----


def test_los_emails_se_normalizan(spark):
    df = customers_df(spark, [(1, "  VICENTE@Example.COM ", "ES", "gold", t(1), t(1))])
    fila = normalize(df, get_table("customers")).collect()[0]
    assert fila["email"] == "vicente@example.com"


def test_los_codigos_de_pais_se_normalizan(spark):
    df = customers_df(spark, [(1, "a@b.com", " es ", "gold", t(1), t(1))])
    fila = normalize(df, get_table("customers")).collect()[0]
    assert fila["country_code"] == "ES"


def test_la_normalizacion_no_inventa_datos(spark):
    """'ESP' no se convierte en 'ES' adivinando. Un dato malo se detecta en la
    validacion, no se arregla a ojo."""
    df = customers_df(spark, [(1, "a@b.com", "esp", "gold", t(1), t(1))])
    fila = normalize(df, get_table("customers")).collect()[0]
    assert fila["country_code"] == "ESP"


def test_la_normalizacion_respeta_los_nulos(spark):
    df = customers_df(spark, [(1, None, None, "gold", t(1), t(1))])
    fila = normalize(df, get_table("customers")).collect()[0]
    assert fila["email"] is None
    assert fila["country_code"] is None


def test_la_normalizacion_no_toca_columnas_que_no_estan_declaradas(spark):
    df = customers_df(spark, [(1, "a@b.com", "ES", "  GOLD  ", t(1), t(1))])
    fila = normalize(df, get_table("customers")).collect()[0]
    assert fila["segment"] == "  GOLD  "


def test_la_normalizacion_no_falla_si_falta_una_columna(spark):
    """Un Bronze antiguo puede no tener una columna que se anadio despues."""
    df = spark.createDataFrame([(1, "  A@B.COM  ")], ["customer_id", "email"])
    fila = normalize(df, get_table("customers")).collect()[0]
    assert fila["email"] == "a@b.com"


# --------------------------------------------------------- deduplicacion ----


def test_se_conserva_la_version_mas_reciente(spark):
    df = customers_df(
        spark,
        [
            (1, "viejo@b.com", "ES", "bronze", t(1), t(1)),
            (1, "nuevo@b.com", "ES", "gold", t(3), t(3)),
            (1, "medio@b.com", "ES", "silver", t(2), t(2)),
        ],
    )
    filas = deduplicate(df, get_table("customers")).collect()
    assert len(filas) == 1
    assert filas[0]["email"] == "nuevo@b.com"
    assert filas[0]["segment"] == "gold"


def test_el_desempate_usa_la_fecha_de_ingesta(spark):
    """Dos filas con el mismo updated_at (duplicado exacto del origen): gana la
    que se ingesto despues. Sin este desempate el resultado dependeria del orden
    en que Spark leyera los ficheros y el job dejaria de ser determinista."""
    df = customers_df(
        spark,
        [
            (1, "primera@b.com", "ES", "bronze", t(1), t(1)),
            (1, "segunda@b.com", "ES", "gold", t(1), t(5)),
        ],
    )
    filas = deduplicate(df, get_table("customers")).collect()
    assert len(filas) == 1
    assert filas[0]["email"] == "segunda@b.com"


def test_claves_distintas_no_se_mezclan(spark):
    df = customers_df(
        spark,
        [
            (1, "uno@b.com", "ES", "gold", t(1), t(1)),
            (2, "dos@b.com", "PT", "bronze", t(1), t(1)),
            (3, "tres@b.com", "FR", "silver", t(1), t(1)),
        ],
    )
    assert deduplicate(df, get_table("customers")).count() == 3


def test_deduplicar_es_idempotente(spark):
    """Deduplicar lo ya deduplicado no cambia nada."""
    df = customers_df(
        spark,
        [
            (1, "a@b.com", "ES", "gold", t(1), t(1)),
            (1, "c@b.com", "ES", "bronze", t(2), t(2)),
        ],
    )
    spec = get_table("customers")
    una_vez = deduplicate(df, spec)
    dos_veces = deduplicate(una_vez, spec)
    assert una_vez.collect() == dos_veces.collect()


def test_el_criterio_de_desempate_sale_de_la_configuracion(spark):
    """Antes estaba escrito a fuego: siempre ganaba el `updated_at` mas alto.

    Eso solo vale si el origen actualiza filas. Este test fija que el criterio
    lo pone la tabla, ordenando por una columna distinta y comprobando que gana
    otra fila que la que ganaria con la watermark.
    """
    df = customers_df(
        spark,
        [
            (1, "viejo@b.com", "ES", "bronze", t(9), t(1)),
            (1, "nuevo@b.com", "ES", "gold", t(1), t(9)),
        ],
    )
    spec = replace(get_table("customers"), dedup_order=["_ingested_at"])
    assert deduplicate(df, spec).collect()[0]["email"] == "nuevo@b.com"


def test_sin_criterio_declarado_el_dedup_se_niega_a_adivinar(spark):
    """Fallar aqui es barato y ruidoso. La alternativa —quedarse con una fila
    cualquiera— descarta datos buenos y no deja rastro."""
    df = customers_df(spark, [(1, "a@b.com", "ES", "gold", t(1), t(1))])
    spec = replace(get_table("customers"), dedup_order=[])
    with pytest.raises(ValueError, match="dedup_order"):
        deduplicate(df, spec)


def test_se_quita_la_particion_de_bronze(spark):
    """`ingestion_date` es la particion fisica de Bronze, no un atributo del
    negocio: en Silver una fila existe una vez, no una por dia de ingesta."""
    df = spark.createDataFrame([(1, "2026-03-15")], ["customer_id", "ingestion_date"])
    assert "ingestion_date" not in drop_bronze_only_columns(df).columns


def test_quitar_la_particion_no_falla_si_no_esta(spark):
    df = spark.createDataFrame([(1,)], ["customer_id"])
    assert drop_bronze_only_columns(df).columns == ["customer_id"]
