"""Tests de la validacion y la cuarentena.

Lo que se fija aqui: que ninguna fila mala se cuele a Silver, que ninguna fila
buena acabe en cuarentena, y que el motivo del rechazo quede registrado. Las
tres cosas fallan en silencio si se rompen.
"""

from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark", reason="Ejecuta los tests dentro del contenedor Glue: make test")

from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.config import get_table  # noqa: E402
from common.quality import ERRORS_COLUMN, report, split  # noqa: E402

# Esquemas explicitos: Spark no puede inferir el tipo de una columna que solo
# tiene nulos, y varios tests prueban precisamente los nulos.
CUSTOMERS = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("email", StringType()),
        StructField("country_code", StringType()),
        StructField("updated_at", TimestampType()),
    ]
)
ORDERS = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("order_date", TimestampType()),
        StructField("total_amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("updated_at", TimestampType()),
    ]
)

T = datetime(2026, 3, 15)


def motivos(cuarentena) -> set[str]:
    """Todos los motivos de rechazo que aparecen, aplanados."""
    return {m for fila in cuarentena.collect() for m in fila[ERRORS_COLUMN]}


# ------------------------------------------------------------- not null -----


def test_una_fila_limpia_pasa(spark):
    df = spark.createDataFrame([(1, "a@b.com", "ES", T)], CUSTOMERS)
    validas, cuarentena = split(df, get_table("customers"))
    assert validas.count() == 1
    assert cuarentena.count() == 0


def test_un_campo_obligatorio_nulo_va_a_cuarentena(spark):
    df = spark.createDataFrame([(1, None, "ES", T)], CUSTOMERS)
    validas, cuarentena = split(df, get_table("customers"))
    assert validas.count() == 0
    assert motivos(cuarentena) == {"email_nulo"}


# -------------------------------------------------------------- patrones ---


def test_un_pais_de_tres_letras_va_a_cuarentena(spark):
    """'ESP' sobrevive a la normalizacion porque el trim y el upper no pueden
    arreglarlo. Es un dato malo, y aqui es donde se detecta."""
    df = spark.createDataFrame([(1, "a@b.com", "ESP", T)], CUSTOMERS)
    _, cuarentena = split(df, get_table("customers"))
    assert motivos(cuarentena) == {"country_code_formato"}


def test_un_email_sin_arroba_va_a_cuarentena(spark):
    df = spark.createDataFrame([(1, "esto-no-es-un-email", "ES", T)], CUSTOMERS)
    _, cuarentena = split(df, get_table("customers"))
    assert motivos(cuarentena) == {"email_formato"}


def test_un_nulo_no_se_reporta_dos_veces(spark):
    """Un email nulo es 'email_nulo', no ademas 'email_formato'. Reportar el
    mismo problema dos veces infla la tasa de cuarentena y despista al leer."""
    df = spark.createDataFrame([(1, None, "ES", T)], CUSTOMERS)
    _, cuarentena = split(df, get_table("customers"))
    assert cuarentena.collect()[0][ERRORS_COLUMN] == ["email_nulo"]


# ------------------------------------------------------------- negativos ---


def test_un_importe_negativo_va_a_cuarentena(spark):
    df = spark.createDataFrame([(1, 10, T, -50.0, "EUR", T)], ORDERS)
    _, cuarentena = split(df, get_table("orders"), {})
    assert "total_amount_negativo" in motivos(cuarentena)


def test_el_cero_es_valido(spark):
    """non_negative significa >= 0. Un pedido de importe cero es raro pero no
    es un error de datos."""
    df = spark.createDataFrame([(1, 10, T, 0.0, "EUR", T)], ORDERS)
    validas, _ = split(df, get_table("orders"), {})
    assert validas.count() == 1


# ------------------------------------------------ integridad referencial ---


def test_una_clave_foranea_huerfana_va_a_cuarentena(spark):
    padres = spark.createDataFrame([(1, "a@b.com", "ES", T)], CUSTOMERS)
    df = spark.createDataFrame([(100, 999, T, 50.0, "EUR", T)], ORDERS)
    validas, cuarentena = split(df, get_table("orders"), {"customers": padres})
    assert validas.count() == 0
    assert "customer_id_huerfano" in motivos(cuarentena)


def test_una_clave_foranea_existente_pasa(spark):
    padres = spark.createDataFrame([(1, "a@b.com", "ES", T)], CUSTOMERS)
    df = spark.createDataFrame([(100, 1, T, 50.0, "EUR", T)], ORDERS)
    validas, cuarentena = split(df, get_table("orders"), {"customers": padres})
    assert validas.count() == 1
    assert cuarentena.count() == 0


def test_una_clave_rechazada_por_otro_motivo_sigue_siendo_valida_como_padre(spark):
    """El caso que motivo cambiar el diseno.

    Un cliente con el email mal escrito va a SU cuarentena, pero existe en el
    origen. Sus pedidos son correctos y no deben caer con el. Validar contra las
    filas supervivientes en vez de contra el origen amplificaba la cuarentena
    casi 7x con los datos de prueba.
    """
    # El universo de claves del origen incluye al cliente 1 aunque su fila vaya
    # a cuarentena por el email.
    universo = spark.createDataFrame([(1, None, "ES", T)], CUSTOMERS)
    df = spark.createDataFrame([(100, 1, T, 50.0, "EUR", T)], ORDERS)
    validas, cuarentena = split(df, get_table("orders"), {"customers": universo})
    assert validas.count() == 1
    assert cuarentena.count() == 0


def test_sin_tabla_padre_no_se_valida_la_referencia(spark):
    """Si el padre no esta disponible no se puede afirmar que la clave sea
    huerfana. Rechazar por no poder comprobar seria peor que no comprobar."""
    df = spark.createDataFrame([(100, 999, T, 50.0, "EUR", T)], ORDERS)
    validas, _ = split(df, get_table("orders"), {})
    assert validas.count() == 1


def test_el_join_de_integridad_no_duplica_filas(spark):
    """Si la tabla padre tiene la clave repetida, un left join mal hecho
    multiplicaria las filas hijas. El distinct lo evita."""
    padres = spark.createDataFrame([(1, "a@b.com", "ES", T), (1, "a@b.com", "ES", T)], CUSTOMERS)
    df = spark.createDataFrame([(100, 1, T, 50.0, "EUR", T)], ORDERS)
    validas, cuarentena = split(df, get_table("orders"), {"customers": padres})
    assert validas.count() + cuarentena.count() == 1


# ------------------------------------------------------ varios problemas ---


def test_se_reportan_todos_los_motivos_a_la_vez(spark):
    """Si un pedido tiene el importe negativo Y el cliente huerfano, quieres
    enterarte de las dos cosas, no arreglar una y descubrir la otra manana."""
    padres = spark.createDataFrame([(1, "a@b.com", "ES", T)], CUSTOMERS)
    df = spark.createDataFrame([(100, 999, T, -50.0, "EUR", T)], ORDERS)
    _, cuarentena = split(df, get_table("orders"), {"customers": padres})
    errores = set(cuarentena.collect()[0][ERRORS_COLUMN])
    assert errores == {"total_amount_negativo", "customer_id_huerfano"}


def test_las_validas_no_llevan_la_columna_de_errores(spark):
    """Silver no debe arrastrar columnas tecnicas de la validacion."""
    df = spark.createDataFrame([(1, "a@b.com", "ES", T)], CUSTOMERS)
    validas, _ = split(df, get_table("customers"))
    assert ERRORS_COLUMN not in validas.columns


def test_no_se_pierde_ninguna_fila(spark):
    """La suma de validas y cuarentena tiene que ser el total. Una fila que no
    esta en ninguno de los dos lados desaparecio sin dejar rastro."""
    df = spark.createDataFrame(
        [
            (1, "a@b.com", "ES", T),
            (2, None, "ES", T),
            (3, "c@b.com", "ESP", T),
            (4, "malo", "PT", T),
        ],
        CUSTOMERS,
    )
    validas, cuarentena = split(df, get_table("customers"))
    assert validas.count() + cuarentena.count() == 4
    assert validas.count() == 1


# --------------------------------------------------------------- informe ---


def test_cada_tabla_declara_su_propio_umbral():
    """No significan lo mismo: un 3% de clientes con el email mal es ruido de un
    formulario; un 3% de productos con precio negativo es un incidente."""
    umbrales = {n: get_table(n).quarantine_threshold for n in ("customers", "products", "orders")}
    assert umbrales["products"] < umbrales["customers"]
    assert all(0 < u < 1 for u in umbrales.values())


def test_la_tasa_se_calcula_bien():
    assert report("orders", total=100, quarantined=5).rate == 0.05


def test_la_tasa_de_un_lote_vacio_no_revienta():
    """Dividir entre cero aqui tumbaria el job en el caso mas tonto posible:
    un dia sin datos."""
    assert report("orders", total=0, quarantined=0).rate == 0.0


def test_el_informe_es_serializable():
    d = report("orders", total=100, quarantined=5).as_dict()
    assert d["valid"] == 95
    assert d["rate"] == 0.05
