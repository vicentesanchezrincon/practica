"""Tests del modelado dimensional.

Lo que se fija aqui son los tres errores clasicos de un modelo estrella, que
comparten una propiedad: **ninguno da error**. El job termina, las tablas se
escriben, y los numeros estan mal.

  1. Claves subrogadas no deterministas: cada reconstruccion reasigna claves y
     los hechos antiguos apuntan a la fila equivocada.
  2. Join contra la dimension sin acotar por fecha: multiplica los hechos.
  3. Comparar atributos con != en vez de <=>: los cambios desde NULL no abren
     version y la historia se pierde.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

pytest.importorskip("pyspark", reason="Ejecuta los tests dentro del contenedor Glue: make test")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.dimensions import (  # noqa: E402
    FAR_FUTURE,
    UNKNOWN_KEY,
    as_of_join,
    scd2_changes,
    surrogate_key,
    unknown_member,
)

DIM = StructType(
    [
        StructField("customer_key", LongType()),
        StructField("customer_id", LongType()),
        StructField("segment", StringType()),
        StructField("country_code", StringType()),
        StructField("valid_from", TimestampType()),
        StructField("valid_to", TimestampType()),
        StructField("is_current", BooleanType()),
    ]
)

ORIGEN = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("segment", StringType()),
        StructField("country_code", StringType()),
        StructField("_cambio", TimestampType()),
    ]
)

HECHOS = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("order_date", TimestampType()),
    ]
)


def t(anyo: int, mes: int = 1, dia: int = 1) -> datetime:
    return datetime(anyo, mes, dia, tzinfo=UTC)


# ------------------------------------------------------- clave subrogada ----


def test_la_clave_subrogada_es_determinista(spark):
    """Si no lo fuera, reconstruir Gold reasignaria claves y los hechos ya
    escritos apuntarian a filas equivocadas."""
    df = spark.createDataFrame([("42",), ("43",)], ["id"])
    primera = [r[0] for r in df.select(surrogate_key("id")).collect()]
    segunda = [r[0] for r in df.select(surrogate_key("id")).collect()]
    assert primera == segunda


def test_claves_distintas_para_entradas_distintas(spark):
    df = spark.createDataFrame([("42",), ("43",)], ["id"])
    claves = [r[0] for r in df.select(surrogate_key("id")).collect()]
    assert len(set(claves)) == 2


def test_la_clave_subrogada_nunca_es_negativa(spark):
    """Las negativas estan reservadas para el miembro desconocido: una colision
    haria que un hecho valido pareciera huerfano."""
    df = spark.createDataFrame([(str(i),) for i in range(300)], ["id"])
    assert df.select(surrogate_key("id")).filter(F.col("abs(xxhash64(id))") < 0).count() == 0


def test_dos_versiones_del_mismo_cliente_tienen_claves_distintas(spark):
    """Es la razon de ser de la clave subrogada: con SCD2 el customer_id se
    repite, asi que no sirve como clave de la dimension."""
    df = spark.createDataFrame([("1", "2026-01-01"), ("1", "2026-06-01")], ["id", "desde"])
    claves = [r[0] for r in df.select(surrogate_key("id", "desde")).collect()]
    assert claves[0] != claves[1]


# --------------------------------------------------- miembro desconocido ----


def test_el_miembro_desconocido_conserva_el_esquema(spark):
    dim = spark.createDataFrame([], DIM)
    fila = unknown_member(dim, "customer_key")
    assert fila.columns == dim.columns
    assert fila.count() == 1


def test_el_miembro_desconocido_usa_la_clave_reservada(spark):
    dim = spark.createDataFrame([], DIM)
    assert unknown_member(dim, "customer_key").collect()[0]["customer_key"] == UNKNOWN_KEY


def test_los_textos_del_miembro_desconocido_son_legibles(spark):
    """Un NULL obliga a quien consulta a saber que significa. '(desconocido)'
    se explica solo."""
    dim = spark.createDataFrame([], DIM)
    assert unknown_member(dim, "customer_key").collect()[0]["segment"] == "(desconocido)"


# ------------------------------------------------------- join temporal ------


def dim_con_dos_versiones(spark):
    """Cliente 1: bronze hasta junio de 2026, gold desde entonces."""
    return spark.createDataFrame(
        [
            (100, 1, "bronze", "ES", t(2020), t(2026, 6, 1), False),
            (200, 1, "gold", "ES", t(2026, 6, 1), FAR_FUTURE, True),
        ],
        DIM,
    )


def test_un_hecho_antiguo_toma_la_version_antigua(spark):
    """LA razon por la que existe el SCD tipo 2: un pedido de marzo se analiza
    con el segmento que el cliente tenia en marzo."""
    hechos = spark.createDataFrame([(1, 1, t(2026, 3, 15))], HECHOS)
    resultado = as_of_join(
        hechos,
        dim_con_dos_versiones(spark),
        natural_key="customer_id",
        fact_date="order_date",
        key_column="customer_key",
    )
    assert resultado.collect()[0]["customer_key"] == 100


def test_un_hecho_reciente_toma_la_version_vigente(spark):
    hechos = spark.createDataFrame([(1, 1, t(2026, 9, 15))], HECHOS)
    resultado = as_of_join(
        hechos,
        dim_con_dos_versiones(spark),
        natural_key="customer_id",
        fact_date="order_date",
        key_column="customer_key",
    )
    assert resultado.collect()[0]["customer_key"] == 200


def test_el_join_temporal_no_multiplica_los_hechos(spark):
    """El error mas caro del modelado dimensional. Un join por clave natural sin
    acotar por fecha devolveria una fila por version, y los ingresos se
    multiplicarian por el numero de versiones sin que nada fallara."""
    hechos = spark.createDataFrame([(1, 1, t(2026, 3, 15)), (2, 1, t(2026, 9, 15))], HECHOS)
    resultado = as_of_join(
        hechos,
        dim_con_dos_versiones(spark),
        natural_key="customer_id",
        fact_date="order_date",
        key_column="customer_key",
    )
    assert resultado.count() == 2


def test_un_hecho_huerfano_va_al_miembro_desconocido(spark):
    """No se descarta: es una venta real y los ingresos tienen que cuadrar."""
    hechos = spark.createDataFrame([(1, 999, t(2026, 3, 15))], HECHOS)
    resultado = as_of_join(
        hechos,
        dim_con_dos_versiones(spark),
        natural_key="customer_id",
        fact_date="order_date",
        key_column="customer_key",
    )
    assert resultado.count() == 1
    assert resultado.collect()[0]["customer_key"] == UNKNOWN_KEY


# ---------------------------------------------------------------- SCD2 ------


def cambios(spark, origen_filas, dim_filas):
    return scd2_changes(
        spark.createDataFrame(origen_filas, ORIGEN),
        spark.createDataFrame(dim_filas, DIM),
        natural_key="customer_id",
        tracked=["segment", "country_code"],
        effective_from="_cambio",
    )


def test_un_cliente_nuevo_abre_version_y_no_cierra_ninguna(spark):
    a_cerrar, a_abrir = cambios(
        spark,
        [(2, "bronze", "PT", t(2026, 5, 1))],
        [(100, 1, "gold", "ES", t(2020), FAR_FUTURE, True)],
    )
    assert a_abrir.count() == 1
    assert a_cerrar.count() == 0


def test_un_atributo_cambiado_cierra_y_abre(spark):
    a_cerrar, a_abrir = cambios(
        spark,
        [(1, "gold", "ES", t(2026, 5, 1))],
        [(100, 1, "bronze", "ES", t(2020), FAR_FUTURE, True)],
    )
    assert a_cerrar.count() == 1
    assert a_abrir.count() == 1


def test_sin_cambios_no_se_toca_nada(spark):
    """Si esto fallara, cada ejecucion abriria versiones nuevas identicas y la
    dimension crecería sin parar."""
    a_cerrar, a_abrir = cambios(
        spark,
        [(1, "gold", "ES", t(2026, 5, 1))],
        [(100, 1, "gold", "ES", t(2020), FAR_FUTURE, True)],
    )
    assert a_cerrar.count() == 0
    assert a_abrir.count() == 0


def test_un_cambio_desde_null_se_detecta(spark):
    """El bug silencioso clasico: `NULL != 'gold'` es NULL en SQL, ni verdadero
    ni falso, asi que con != este cambio no abriria version."""
    a_cerrar, a_abrir = cambios(
        spark,
        [(1, "gold", "ES", t(2026, 5, 1))],
        [(100, 1, None, "ES", t(2020), FAR_FUTURE, True)],
    )
    assert a_cerrar.count() == 1
    assert a_abrir.count() == 1


def test_un_cambio_a_null_tambien_se_detecta(spark):
    a_cerrar, a_abrir = cambios(
        spark,
        [(1, None, "ES", t(2026, 5, 1))],
        [(100, 1, "gold", "ES", t(2020), FAR_FUTURE, True)],
    )
    assert a_cerrar.count() == 1


def test_las_versiones_ya_cerradas_no_se_comparan(spark):
    """Solo la version vigente se compara. Si se compararan las cerradas, un
    cliente que vuelve a un segmento anterior abriria versiones sin sentido."""
    a_cerrar, a_abrir = cambios(
        spark,
        [(1, "gold", "ES", t(2026, 5, 1))],
        [
            (100, 1, "bronze", "ES", t(2020), t(2025), False),
            (200, 1, "gold", "ES", t(2025), FAR_FUTURE, True),
        ],
    )
    assert a_cerrar.count() == 0
    assert a_abrir.count() == 0
