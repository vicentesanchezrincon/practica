"""Tests de la sesionizacion.

Lo que se fija aqui es que una visita se derive del TIEMPO y no del
identificador que manda el cliente. Confundir las dos cosas no rompe nada: da
sesiones de seis horas con cinco de pausa dentro, y unas metricas creibles y
falsas.
"""

from __future__ import annotations

from datetime import datetime, timedelta

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

from common.sessions import HUECO_POR_DEFECTO, marcar_sesiones, resumir_sesiones  # noqa: E402

EVENTOS = StructType(
    [
        StructField("session_id", StringType()),
        StructField("event_time", TimestampType()),
        StructField("customer_id", LongType()),
        StructField("event_type", StringType()),
        StructField("device", StringType()),
        StructField("utm_source", StringType()),
        StructField("amount", DoubleType()),
    ]
)

BASE = datetime(2026, 3, 15, 10, 0, 0)


def t(minutos: int) -> datetime:
    return BASE + timedelta(minutes=minutos)


def eventos_df(spark, filas):
    return spark.createDataFrame(filas, EVENTOS)


def visitas(df) -> set[str]:
    return {f["session_key"] for f in df.collect()}


# ------------------------------------------------ derivar, no confiar ---


def test_una_pausa_larga_parte_la_visita_en_dos(spark):
    """El caso que justifica todo el modulo: misma cookie, dos visitas.

    El visitante deja la pestana abierta y vuelve dos horas despues. El
    `session_id` es el mismo porque la cookie sigue viva, pero no fue una
    visita de dos horas.
    """
    df = eventos_df(
        spark,
        [
            ("s1", t(0), 1, "page_view", "movil", "organico", None),
            ("s1", t(5), 1, "product_view", "movil", "organico", None),
            ("s1", t(125), 1, "page_view", "movil", "organico", None),
        ],
    )
    assert len(visitas(marcar_sesiones(df))) == 2


def test_los_eventos_seguidos_son_una_sola_visita(spark):
    df = eventos_df(
        spark,
        [
            ("s1", t(0), 1, "page_view", "movil", "organico", None),
            ("s1", t(10), 1, "product_view", "movil", "organico", None),
            ("s1", t(25), 1, "add_to_cart", "movil", "organico", None),
        ],
    )
    assert len(visitas(marcar_sesiones(df))) == 1


def test_el_hueco_es_inclusive_en_su_limite(spark):
    """Exactamente 30 minutos NO abre visita nueva; 30 y un segundo, si. Sin
    fijarlo, el limite documentado y el aplicado se separan y nadie lo nota."""
    justo = eventos_df(
        spark,
        [
            ("s1", t(0), 1, "page_view", "movil", "organico", None),
            ("s1", t(30), 1, "page_view", "movil", "organico", None),
        ],
    )
    assert len(visitas(marcar_sesiones(justo))) == 1

    pasado = eventos_df(
        spark,
        [
            ("s1", BASE, 1, "page_view", "movil", "organico", None),
            (
                "s1",
                BASE + timedelta(minutes=30, seconds=1),
                1,
                "page_view",
                "movil",
                "organico",
                None,
            ),  # fmt: skip
        ],
    )
    assert len(visitas(marcar_sesiones(pasado))) == 2


def test_dos_visitantes_no_se_mezclan(spark):
    df = eventos_df(
        spark,
        [
            ("s1", t(0), 1, "page_view", "movil", "organico", None),
            ("s2", t(1), 2, "page_view", "escritorio", "google_ads", None),
        ],
    )
    assert len(visitas(marcar_sesiones(df))) == 2


def test_el_hueco_es_configurable(spark):
    """Cambiar el umbral cambia TODAS las metricas de sesion a la vez, asi que
    tiene que estar declarado y ser explicito, no escondido en el codigo."""
    df = eventos_df(
        spark,
        [
            ("s1", t(0), 1, "page_view", "movil", "organico", None),
            ("s1", t(45), 1, "page_view", "movil", "organico", None),
        ],
    )
    assert len(visitas(marcar_sesiones(df, hueco=timedelta(hours=1)))) == 1
    assert len(visitas(marcar_sesiones(df, hueco=timedelta(minutes=30)))) == 2


def test_el_hueco_por_defecto_es_la_convencion_del_sector():
    assert timedelta(minutes=30) == HUECO_POR_DEFECTO


# ------------------------------------------------------------ resumen ---


def test_la_duracion_no_cuenta_la_pausa(spark):
    """La consecuencia medible de sesionizar bien. Agrupando por session_id
    esta visita duraria 125 minutos; son dos visitas de 5 y de 0."""
    df = eventos_df(
        spark,
        [
            ("s1", t(0), 1, "page_view", "movil", "organico", None),
            ("s1", t(5), 1, "product_view", "movil", "organico", None),
            ("s1", t(125), 1, "page_view", "movil", "organico", None),
        ],
    )
    duraciones = sorted(f["duracion_segundos"] for f in resumir_sesiones(df).collect())
    assert duraciones == [0, 300]


def test_el_embudo_marca_hasta_donde_llego_la_visita(spark):
    df = eventos_df(
        spark,
        [
            ("s1", t(0), 1, "page_view", "movil", "organico", None),
            ("s1", t(1), 1, "product_view", "movil", "organico", None),
            ("s1", t(2), 1, "add_to_cart", "movil", "organico", None),
            ("s1", t(3), 1, "checkout_start", "movil", "organico", None),
            ("s1", t(4), 1, "purchase", "movil", "organico", 120.5),
        ],
    )
    fila = resumir_sesiones(df).collect()[0]
    assert (fila["anadio_al_carrito"], fila["inicio_compra"], fila["compro"]) == (True, True, True)
    assert fila["importe"] == 120.5
    assert fila["productos_vistos"] == 1


def test_una_visita_que_no_compra_tiene_importe_cero_y_no_nulo(spark):
    """Un nulo aqui contamina cualquier suma posterior sin avisar."""
    df = eventos_df(spark, [("s1", t(0), 1, "page_view", "movil", "organico", None)])
    fila = resumir_sesiones(df).collect()[0]
    assert fila["importe"] == 0.0
    assert fila["compro"] is False


def test_una_visita_anonima_se_marca_como_tal(spark):
    """No es lo mismo "no sabemos quien era" que "fue un cliente concreto".
    En Gold, las anonimas caen en el miembro desconocido en vez de perderse."""
    df = eventos_df(spark, [("s1", t(0), None, "page_view", "movil", "organico", None)])
    fila = resumir_sesiones(df).collect()[0]
    assert fila["anonima"] is True
    assert fila["customer_id"] is None


def test_si_el_visitante_se_identifica_a_mitad_la_visita_deja_de_ser_anonima(spark):
    """Empieza navegando sin login y hace login para comprar. La visita es de
    ese cliente entera, no media."""
    df = eventos_df(
        spark,
        [
            ("s1", t(0), None, "page_view", "movil", "organico", None),
            ("s1", t(2), None, "product_view", "movil", "organico", None),
            ("s1", t(4), 77, "purchase", "movil", "organico", 30.0),
        ],
    )
    fila = resumir_sesiones(df).collect()[0]
    assert fila["customer_id"] == 77
    assert fila["anonima"] is False


def test_la_atribucion_es_del_primer_contacto_de_la_visita(spark):
    """La campana que trajo al visitante, no la ultima pagina que vio."""
    df = eventos_df(
        spark,
        [
            ("s1", t(0), 1, "page_view", "movil", "newsletter", None),
            ("s1", t(2), 1, "product_view", "movil", None, None),
        ],
    )
    assert resumir_sesiones(df).collect()[0]["utm_source"] == "newsletter"


def test_cada_visita_derivada_es_una_fila(spark):
    df = eventos_df(
        spark,
        [
            ("s1", t(0), 1, "page_view", "movil", "organico", None),
            ("s1", t(200), 1, "page_view", "movil", "organico", None),
            ("s2", t(0), 2, "page_view", "tablet", "instagram", None),
        ],
    )
    assert resumir_sesiones(df).count() == 3
