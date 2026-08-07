"""Tests de la serie temporal: lo que rompe respecto a una tabla de entidades.

No prueban Spark. Prueban las decisiones que distinguen un flujo de eventos de
las cuatro tablas maestras, y que son justo las que no dan error cuando se
toman mal:

  * leer por hora de llegada y no por hora del hecho,
  * retroceder una ventana en cada pasada para no perder lo que llega tarde,
  * quedarse con la PRIMERA llegada de un evento reenviado, no con la ultima,
  * no exigir cliente a un visitante anonimo,
  * y el cambio de hora, que convierte una hora local en dos instantes.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from common.config import EVENT_LOOKBACK, INGESTION_ORDER, get_table
from common.tiempo import MADRID, hora_ambigua, hora_inexistente, transiciones_horarias

EVENTOS = get_table("web_events")


# ------------------------------------------------------- las dos marcas ---


def test_la_watermark_es_la_hora_de_llegada_no_la_del_hecho():
    """La decision central de esta fuente.

    Con `event_time` como marca, un evento de ayer que llega hoy nace ya por
    detras del watermark: no se lee nunca, no falla nada, y el hueco solo se
    descubre cuadrando totales meses despues.
    """
    assert EVENTOS.source.watermark_column == "received_at"


def test_hay_ventana_de_reproceso_y_cubre_el_peor_retraso():
    """El generador produce retrasos de hasta 30 horas. La ventana tiene que
    ser mayor, o esos eventos se pierden en la ejecucion siguiente."""
    assert EVENTOS.source.lookback >= timedelta(hours=30)
    assert EVENTOS.source.lookback == EVENT_LOOKBACK


def test_las_tablas_de_entidades_no_necesitan_ventana():
    """La ventana no es gratis: introduce duplicados a proposito. Donde el
    incremental ya es fiable, no se paga."""
    for tabla in ("customers", "orders"):
        assert get_table(tabla).source.lookback == timedelta(0)


def test_la_lectura_retrocede_la_ventana_desde_el_watermark():
    from jobs.bronze_ingest import lectura_desde

    watermark = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
    assert lectura_desde(EVENTOS, watermark) == watermark - EVENT_LOOKBACK
    assert lectura_desde(get_table("orders"), watermark) == watermark


# --------------------------------------------------------------- dedup ---


def test_de_un_evento_reenviado_gana_la_primera_llegada():
    """Un reenvio no es una version mas nueva del hecho: es el mismo hecho
    contado dos veces. Quedarse con la ultima llegada infla la latencia medida
    y nada falla."""
    assert EVENTOS.dedup_keep == "primera"
    assert EVENTOS.dedup_order == ["received_at"]


def test_de_una_entidad_actualizada_gana_la_ultima_version():
    """El caso contrario, para que se vea que son dos situaciones distintas y
    no una preferencia."""
    assert get_table("customers").dedup_keep == "ultima"


def test_la_clave_de_negocio_es_el_id_del_evento():
    """No hay clave secuencial: la genera el cliente. Es lo que permite
    reconocer un reenvio, y lo que impide trocear la lectura por rango."""
    assert EVENTOS.business_key == ["event_id"]


# ------------------------------------------------------ nulos legitimos ---


def test_el_cliente_anonimo_no_es_un_dato_malo():
    """La regla correcta en `orders` seria aqui un error de diseno: dos tercios
    del trafico de una tienda no ha iniciado sesion."""
    assert "customer_id" not in EVENTOS.not_null
    assert "customer_id" in get_table("orders").not_null


def test_pero_si_el_cliente_viene_tiene_que_existir():
    """No exigirlo no es lo mismo que no comprobarlo. Un customer_id con valor
    que no apunta a nadie sigue siendo un huerfano."""
    assert EVENTOS.references["customer_id"] == ("customers", "customer_id")


def test_los_eventos_se_ingestan_despues_de_sus_padres():
    """Si `web_events` entrase antes que `customers`, la validacion referencial
    no tendria contra que comparar y marcaria huerfano a todo el mundo."""
    orden = INGESTION_ORDER.index
    assert orden("web_events") > orden("customers")
    assert orden("web_events") > orden("products")


# ------------------------------------------------------- cambio de hora ---


def test_se_detectan_las_dos_transiciones_de_un_ano():
    """Un ano natural tiene exactamente dos: una que adelanta y otra que
    atrasa."""
    cambios = transiciones_horarias(date(2025, 8, 1), date(2026, 7, 31))
    assert len(cambios) == 2
    assert {sentido for _, sentido in cambios} == {"adelanta", "atrasa"}


def test_las_transiciones_caen_donde_dice_la_norma_europea():
    """Ultimo domingo de marzo y ultimo domingo de octubre."""
    cambios = dict(transiciones_horarias(date(2025, 8, 1), date(2026, 7, 31)))
    assert cambios[date(2025, 10, 26)] == "atrasa"
    assert cambios[date(2026, 3, 29)] == "adelanta"


def test_no_se_detecta_ninguna_transicion_en_un_mes_tranquilo():
    assert transiciones_horarias(date(2026, 6, 1), date(2026, 6, 30)) == []


def test_la_hora_que_se_repite_son_dos_instantes_distintos():
    """El nucleo de la trampa. Las 02:30 del 26 de octubre de 2025 existen dos
    veces en Madrid, y son dos momentos separados por una hora real.

    Dos eventos ahi NO son un duplicado. Deduplicar por hora local se comeria
    la mitad, y el informe de calidad no diria nada porque no falla nada.
    """
    antes = datetime(2025, 10, 26, 2, 30, tzinfo=MADRID, fold=0).astimezone(UTC)
    despues = datetime(2025, 10, 26, 2, 30, tzinfo=MADRID, fold=1).astimezone(UTC)
    assert antes != despues
    assert despues - antes == timedelta(hours=1)


def test_se_reconoce_la_hora_que_ocurre_dos_veces():
    ambigua = datetime(2025, 10, 26, 2, 30, tzinfo=MADRID)
    normal = datetime(2025, 10, 26, 5, 30, tzinfo=MADRID)
    assert hora_ambigua(ambigua)
    assert not hora_ambigua(normal)


def test_la_hora_que_no_existe_no_se_confunde_con_la_que_se_repite():
    """Las 02:30 del dia que el reloj se adelanta no ocurren NI UNA vez.

    Son problemas opuestos —uno duplica y el otro borra— y la implementacion
    evidente los confunde: comparar los desfases de fold=0 y fold=1 da True
    para los dos, porque `fold` cubre las dos anomalias. Lo que las separa es
    el signo de la diferencia.
    """
    imposible = datetime(2026, 3, 29, 2, 30, tzinfo=MADRID)
    assert hora_inexistente(imposible)
    assert not hora_ambigua(imposible)


def test_una_hora_corriente_no_es_ni_lo_uno_ni_lo_otro():
    normal = datetime(2026, 6, 15, 2, 30, tzinfo=MADRID)
    assert not hora_ambigua(normal)
    assert not hora_inexistente(normal)


def test_en_utc_esa_misma_hora_no_se_repite():
    """Por eso se guarda todo en UTC y la hora local es solo una vista: en UTC
    el tiempo avanza siempre, sin saltos ni repeticiones."""
    horas_utc = {
        datetime(2025, 10, 26, 2, 30, tzinfo=MADRID, fold=f).astimezone(UTC).hour for f in (0, 1)
    }
    assert len(horas_utc) == 2


def test_la_particion_de_silver_usa_la_hora_del_hecho():
    """Se consulta por cuando OCURRIO algo, no por cuando llego. Particionar por
    `received_at` obligaria a leer particiones de mas en toda consulta de
    negocio."""
    assert EVENTOS.silver_partition == "days(event_time)"


# ------------------------------------------------------------- volumen ---


def test_se_trocea_la_lectura_por_tiempo_al_no_haber_clave_numerica():
    """Spark tambien paraleliza por timestamp. Sin esto, millones de filas
    entran por un solo hilo."""
    assert EVENTOS.source.partition_column == "received_at"


def test_el_limite_del_rango_va_en_un_formato_que_spark_entiende():
    """`str(datetime)` produce '...+00:00', con dos puntos en el huso, y el
    parser de timestamps de Spark no lo acepta. Falla en ejecucion y solo con
    origenes particionados por tiempo."""
    from jobs.bronze_ingest import limite

    formateado = limite(datetime(2026, 3, 15, 12, 0, 30, tzinfo=UTC))
    assert formateado == "2026-03-15 12:00:30.000000"
    assert "+" not in formateado
    assert limite(42) == "42"
