"""Tests de la validacion y la cuarentena.

Lo que se fija aqui: que ninguna fila mala se cuele a Silver, que ninguna fila
buena acabe en cuarentena, y que el motivo del rechazo quede registrado. Las
tres cosas fallan en silencio si se rompen.
"""

from __future__ import annotations

from dataclasses import replace
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

from common.config import ORDER_STATUS, SEGMENTS, get_table  # noqa: E402
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

# ORDERS a proposito NO trae status ni created_at: las reglas que dependen de
# columnas ausentes tienen que callarse, y varios tests de arriba lo comprueban
# sin saberlo. Para las reglas nuevas hace falta la tabla completa.
ORDERS_COMPLETO = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("order_date", TimestampType()),
        StructField("total_amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("status", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
    ]
)
ORDER_ITEMS = StructType(
    [
        StructField("order_item_id", LongType()),
        StructField("order_id", LongType()),
        StructField("product_id", LongType()),
        StructField("quantity", LongType()),
        StructField("unit_price", DoubleType()),
        StructField("line_amount", DoubleType()),
        StructField("created_at", TimestampType()),
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


# ----------------------------------------------------------------- rango ---
# `ranges` existe porque `non_negative` no llega a todo: hay columnas con techo,
# y hay columnas donde un negativo es correcto (una devolucion).


def test_un_valor_por_encima_del_maximo_va_a_cuarentena(spark):
    """order_items pone techo de 1000 a quantity: mil unidades de la misma
    linea no es un pedido, es un error de tecleo."""
    df = spark.createDataFrame([(1, 10, 20, 5000, 2.0, 10.0, T, T)], ORDER_ITEMS)
    validas, cuarentena = split(df, get_table("order_items"))
    assert validas.count() == 0
    assert "quantity_sobre_maximo" in motivos(cuarentena)


def test_un_valor_justo_en_el_maximo_es_valido(spark):
    """Los extremos son inclusive. Si no lo fueran, el limite documentado y el
    aplicado se diferenciarian en uno y nadie lo notaria hasta el borde."""
    df = spark.createDataFrame([(1, 10, 20, 1000, 2.0, 10.0, T, T)], ORDER_ITEMS)
    validas, _ = split(df, get_table("order_items"))
    assert validas.count() == 1


def test_el_motivo_distingue_pasarse_de_quedarse_corto(spark):
    """Cada extremo es su propia regla, no un OR. Quien mira la cuarentena
    quiere saber hacia que lado se fue el valor sin abrir la fila."""
    spec = replace(get_table("order_items"), ranges={"quantity": (2, 10)})
    df = spark.createDataFrame([(1, 10, 20, 1, 2.0, 10.0, T, T)], ORDER_ITEMS)
    _, cuarentena = split(df, spec)
    assert "quantity_bajo_minimo" in motivos(cuarentena)
    assert "quantity_sobre_maximo" not in motivos(cuarentena)


def test_un_rango_sin_minimo_no_rechaza_negativos(spark):
    """El caso de la liquidacion: una devolucion es un importe negativo bueno.
    Con (None, x) el suelo simplemente no existe."""
    spec = replace(get_table("order_items"), non_negative=[], ranges={"line_amount": (None, 999.0)})
    df = spark.createDataFrame([(1, 10, 20, 1, 2.0, -45.0, T, T)], ORDER_ITEMS)
    validas, _ = split(df, spec)
    assert validas.count() == 1


# ------------------------------------------------------------ enumerados ---


def test_un_valor_fuera_del_vocabulario_va_a_cuarentena(spark):
    """No suele ser un dato sucio: es el origen avisando de que ha cambiado."""
    df = spark.createDataFrame([(1, 7, T, 10.0, "EUR", "devuelto_parcial", T, T)], ORDERS_COMPLETO)
    validas, cuarentena = split(df, get_table("orders"))
    assert validas.count() == 0
    assert "status_valor_no_admitido" in motivos(cuarentena)


def test_los_valores_del_vocabulario_pasan(spark):
    df = spark.createDataFrame([(1, 7, T, 10.0, "EUR", "shipped", T, T)], ORDERS_COMPLETO)
    validas, cuarentena = split(df, get_table("orders"))
    assert cuarentena.count() == 0
    assert validas.count() == 1


def test_un_enumerado_nulo_no_duplica_el_motivo(spark):
    """Mismo criterio que en los patrones: de los nulos ya se encarga not_null.
    Reportar el mismo problema dos veces hace ilegible la cuarentena."""
    spec = replace(get_table("orders"), not_null=["order_id", "status"])
    df = spark.createDataFrame([(1, 7, T, 10.0, "EUR", None, T, T)], ORDERS_COMPLETO)
    _, cuarentena = split(df, spec)
    assert motivos(cuarentena) == {"status_nulo"}


def test_el_vocabulario_declarado_es_el_que_usa_el_generador():
    """La regla mas barata de romper: anadir un estado al generador y olvidarse
    de la lista de validacion. Silver mandaria a cuarentena datos buenos."""
    assert set(get_table("orders").allowed_values["status"]) == set(ORDER_STATUS)
    assert set(get_table("customers").allowed_values["segment"]) == set(SEGMENTS)


# ------------------------------------------------- coherencia de tiempos ---
# Lo que ninguna regla de una sola columna puede ver: un valor plausible por si
# mismo que contradice a otro.


def test_un_created_at_posterior_al_updated_at_va_a_cuarentena(spark):
    creado = datetime(2026, 3, 15, 12, 0, 30)
    actualizado = datetime(2026, 3, 15, 12, 0, 0)
    df = spark.createDataFrame(
        [(1, 7, T, 10.0, "EUR", "paid", creado, actualizado)], ORDERS_COMPLETO
    )
    validas, cuarentena = split(df, get_table("orders"))
    assert validas.count() == 0
    assert "created_at_posterior_a_updated_at" in motivos(cuarentena)


def test_la_tolerancia_absuelve_una_desviacion_minima(spark):
    """Dos relojes nunca van iguales. Sin margen, la regla manda a cuarentena
    filas correctas y acaba desactivada por inutil, que es lo peor que le puede
    pasar a una regla de calidad."""
    creado = datetime(2026, 3, 15, 12, 0, 0, 500_000)
    actualizado = datetime(2026, 3, 15, 12, 0, 0)
    df = spark.createDataFrame(
        [(1, 7, T, 10.0, "EUR", "paid", creado, actualizado)], ORDERS_COMPLETO
    )
    validas, _ = split(df, get_table("orders"))
    assert validas.count() == 1


def test_el_orden_normal_de_los_tiempos_pasa(spark):
    creado = datetime(2026, 3, 15, 12, 0, 0)
    actualizado = datetime(2026, 3, 16, 9, 30, 0)
    df = spark.createDataFrame(
        [(1, 7, T, 10.0, "EUR", "paid", creado, actualizado)], ORDERS_COMPLETO
    )
    validas, cuarentena = split(df, get_table("orders"))
    assert cuarentena.count() == 0
    assert validas.count() == 1


def test_sin_una_de_las_dos_columnas_la_regla_no_se_aplica(spark):
    """ORDERS no trae created_at. Una regla que necesita dos columnas y solo
    encuentra una tiene que callarse, no reventar el job."""
    df = spark.createDataFrame([(1, 7, T, 10.0, "EUR", T)], ORDERS)
    validas, _ = split(df, get_table("orders"))
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
