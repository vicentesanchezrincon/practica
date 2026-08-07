"""Tests de la conciliacion entre Postgres y el proveedor de pagos.

Es la unica metrica del proyecto que cruza dos ORIGENES distintos, y por tanto
la unica capaz de detectar un fallo que ninguno de los dos ve por separado:
cada fuente sigue siendo internamente coherente aunque falte un fichero entero.

Los cuatro casos que tiene que cazar, y que son la razon de que exista:

  * un fichero que no llego,
  * un fichero procesado dos veces,
  * la referencia del proveedor cruzada sin normalizar,
  * y las fechas leidas con el formato equivocado.
"""

from __future__ import annotations

from datetime import date

import pytest

pytest.importorskip("pyspark", reason="Ejecuta los tests dentro del contenedor Glue: make test")

from pyspark.sql.types import (  # noqa: E402
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common.conciliacion import TOLERANCIA, conciliar, resumen  # noqa: E402

PEDIDOS = StructType(
    [
        StructField("order_id", LongType()),
        StructField("order_date", DateType()),
        StructField("total_amount", DoubleType()),
    ]
)
LIQUIDACIONES = StructType(
    [
        StructField("fecha_operacion", DateType()),
        StructField("tipo", StringType()),
        StructField("importe", DoubleType()),
        StructField("comision", DoubleType()),
    ]
)

DIA = date(2026, 3, 15)
OTRO = date(2026, 3, 16)


def pedidos_df(spark, filas):
    return spark.createDataFrame(filas, PEDIDOS)


def liquidaciones_df(spark, filas):
    return spark.createDataFrame(filas, LIQUIDACIONES)


def por_dia(df) -> dict:
    return {f["dia"]: f for f in df.collect()}


# --------------------------------------------------------------- el cuadre ---


def test_un_dia_que_cuadra_se_marca_como_tal(spark):
    """100 de ventas, 100 cobrados: no falta ni sobra dinero."""
    fila = por_dia(
        conciliar(
            pedidos_df(spark, [(1, DIA, 100.0)]),
            liquidaciones_df(spark, [(DIA, "PAGO", 100.0, 2.25)]),
        )
    )[DIA]
    assert fila["descuadre"] == 0.0
    assert fila["cuadra"] is True


def test_la_comision_se_informa_pero_no_cuenta_como_descuadre(spark):
    """La comision es el precio del servicio, no dinero que falte.

    Metiendola en el cuadre, TODOS los dias saldrian descuadrados por un motivo
    perfectamente normal, y la senal dejaria de servir para lo unico que
    importa. Se informa aparte, en `esperado`.
    """
    fila = por_dia(
        conciliar(
            pedidos_df(spark, [(1, DIA, 100.0)]),
            liquidaciones_df(spark, [(DIA, "PAGO", 100.0, 2.25)]),
        )
    )[DIA]
    assert fila["comisiones"] == 2.25
    assert fila["esperado"] == 97.75
    assert fila["cuadra"] is True


def test_una_devolucion_no_se_cuenta_como_cobro(spark):
    """Un importe negativo es correcto aqui, pero no es un ingreso: si se
    sumara con los pagos, el dia parecería cuadrar cuando no cuadra."""
    fila = por_dia(
        conciliar(
            pedidos_df(spark, [(1, DIA, 100.0), (2, DIA, 50.0)]),
            liquidaciones_df(
                spark,
                [(DIA, "PAGO", 100.0, 2.15), (DIA, "DEVOL", -50.0, -1.20)],
            ),
        )
    )[DIA]
    assert fila["devoluciones"] == 50.0
    assert fila["importe_liquidado"] == 50.0


def test_un_descuadre_grande_se_detecta(spark):
    """Falta la mitad del dinero del dia. Con la tolerancia por defecto, no
    pasa."""
    fila = por_dia(
        conciliar(
            pedidos_df(spark, [(1, DIA, 1000.0)]),
            liquidaciones_df(spark, [(DIA, "PAGO", 500.0, 10.0)]),
        )
    )[DIA]
    assert fila["cuadra"] is False
    assert fila["descuadre_relativo"] > TOLERANCIA


def test_la_comision_normal_entra_dentro_de_la_tolerancia(spark):
    """El 1,9% mas 25 centimos que cobra el proveedor no es un descuadre: es el
    coste del servicio. Una tolerancia de 0 daria alarma todos los dias y la
    senal acabaria ignorada."""
    fila = por_dia(
        conciliar(
            pedidos_df(spark, [(1, DIA, 1000.0)]),
            liquidaciones_df(spark, [(DIA, "PAGO", 1000.0, 19.25)]),
        )
    )[DIA]
    assert fila["cuadra"] is True


# --------------------------------------------- los cuatro fallos silenciosos ---


def test_un_dia_con_ventas_y_sin_liquidacion_aparece_en_el_informe(spark):
    """El fichero que no llego.

    El join es FULL OUTER justo por esto: con un `inner`, el dia desapareceria
    del informe y el problema seria literalmente invisible.

    El dia sin fichero va EN MEDIO del periodo cubierto a proposito. Puesto al
    final quedaria fuera de la ventana de conciliacion y el test pasaria por el
    motivo equivocado.
    """
    ultimo = date(2026, 3, 17)
    filas = por_dia(
        conciliar(
            pedidos_df(spark, [(1, DIA, 100.0), (2, OTRO, 200.0), (3, ultimo, 50.0)]),
            liquidaciones_df(spark, [(DIA, "PAGO", 100.0, 2.15), (ultimo, "PAGO", 50.0, 1.20)]),
        )
    )
    assert OTRO in filas
    assert filas[OTRO]["importe_liquidado"] == 0.0
    assert filas[OTRO]["cuadra"] is False


def test_un_dia_con_liquidacion_y_sin_ventas_tampoco_pasa(spark):  # noqa: D401
    """Dinero que llega de ninguna parte: normalmente, un fichero procesado dos
    veces o con la fecha mal leida. No se puede calcular descuadre relativo
    —no hay denominador— y aun asi tiene que fallar."""
    fila = por_dia(
        conciliar(
            pedidos_df(spark, [(1, DIA, 100.0)]),
            liquidaciones_df(spark, [(DIA, "PAGO", 100.0, 2.15), (OTRO, "PAGO", 500.0, 9.75)]),
        )
    )[OTRO]
    assert fila["importe_pedidos"] == 0.0
    assert fila["descuadre_relativo"] is None
    assert fila["cuadra"] is False


def test_un_fichero_procesado_dos_veces_duplica_el_importe_y_se_ve(spark):
    """El caso de la idempotencia rota. Los movimientos entran dos veces y el
    dia liquida el doble de lo que se vendio."""
    fila = por_dia(
        conciliar(
            pedidos_df(spark, [(1, DIA, 100.0)]),
            liquidaciones_df(spark, [(DIA, "PAGO", 100.0, 2.15), (DIA, "PAGO", 100.0, 2.15)]),
        )
    )[DIA]
    assert fila["importe_liquidado"] == 200.0
    assert fila["cuadra"] is False


def test_los_dias_sin_dato_valen_cero_y_no_nulo(spark):
    """Con nulos, cualquier resta posterior se propaga a NULL y el dia
    problematico DESAPARECE del informe. Es justo el dia que hay que mirar."""
    fila = por_dia(
        conciliar(
            pedidos_df(spark, [(1, DIA, 100.0)]),
            liquidaciones_df(spark, []),
        )
    )[DIA]
    for columna in ("cobros", "devoluciones", "comisiones", "importe_liquidado"):
        assert fila[columna] == 0.0, columna


# ---------------------------------------------------------------- el grano ---


def test_no_se_multiplican_filas_al_cruzar_las_dos_fuentes(spark):
    """Un pedido puede tener varios movimientos (un pago y su devolucion).

    Cruzando al detalle en vez de agregar cada lado por separado, el importe de
    pedidos se contaria una vez por movimiento y el total saldria inflado sin
    que nada fallara.
    """
    fila = por_dia(
        conciliar(
            pedidos_df(spark, [(1, DIA, 100.0)]),
            liquidaciones_df(
                spark,
                [(DIA, "PAGO", 100.0, 2.15), (DIA, "DEVOL", -100.0, -2.15)],
            ),
        )
    )[DIA]
    assert fila["importe_pedidos"] == 100.0
    assert fila["pedidos"] == 1
    assert fila["movimientos"] == 2


def test_hay_una_fila_por_dia_y_no_por_movimiento(spark):
    conciliacion = conciliar(
        pedidos_df(spark, [(1, DIA, 100.0), (2, OTRO, 100.0)]),
        liquidaciones_df(
            spark,
            [
                (DIA, "PAGO", 60.0, 1.39),
                (DIA, "PAGO", 40.0, 1.01),
                (OTRO, "PAGO", 100.0, 2.15),
            ],
        ),
    )
    assert conciliacion.count() == 2


# --------------------------------------------------------------- el resumen ---


def test_el_resumen_cuenta_los_dias_descuadrados(spark):
    conciliacion = conciliar(
        pedidos_df(spark, [(1, DIA, 100.0), (2, OTRO, 1000.0)]),
        liquidaciones_df(spark, [(DIA, "PAGO", 100.0, 2.15), (OTRO, "PAGO", 100.0, 2.15)]),
    )
    cifras = resumen(conciliacion)
    assert cifras["dias"] == 2
    # OTRO vendio 1000 y solo se cobraron 100: falta el 90%.
    assert cifras["dias_descuadrados"] == 1
    assert cifras["tolerancia"] == TOLERANCIA


def test_el_resumen_es_serializable_a_json(spark):
    """Lo lee Step Functions desde S3: tiene que ser un dict de tipos simples,
    no un DataFrame ni objetos de Spark."""
    import json

    cifras = resumen(
        conciliar(
            pedidos_df(spark, [(1, DIA, 100.0)]),
            liquidaciones_df(spark, [(DIA, "PAGO", 100.0, 2.15)]),
        )
    )
    assert json.loads(json.dumps(cifras))["dias"] == 1


# ------------------------------------------------- el periodo que se cubre ---


def test_solo_se_concilia_el_periodo_que_el_proveedor_ha_liquidado(spark):
    """Lo encontro la primera ejecucion real: 367 de 368 dias "descuadrados".

    Los ficheros cubrian 60 dias y los pedidos un ano entero, asi que todo el
    historico anterior salia al 100% de descuadre. Ese es el modo de fallo de
    casi cualquier alarma de calidad: no que no detecte, sino que detecte tanto
    que deje de mirarse.
    """
    antiguo = date(2026, 1, 10)
    conciliacion = conciliar(
        pedidos_df(spark, [(1, antiguo, 500.0), (2, DIA, 100.0)]),
        liquidaciones_df(spark, [(DIA, "PAGO", 100.0, 2.15)]),
    )
    dias = por_dia(conciliacion)
    assert antiguo not in dias, "un dia fuera de la cobertura no es un descuadre"
    assert DIA in dias


def test_un_dia_sin_liquidar_DENTRO_de_la_cobertura_si_es_un_fallo(spark):
    """El recorte no puede tapar lo que se quiere detectar.

    Un dia sin fichero en medio del periodo cubierto es exactamente el hueco en
    la secuencia, y tiene que seguir apareciendo.
    """
    enmedio = date(2026, 3, 16)
    dias = por_dia(
        conciliar(
            pedidos_df(spark, [(1, DIA, 100.0), (2, enmedio, 300.0), (3, date(2026, 3, 17), 50.0)]),
            liquidaciones_df(
                spark,
                [(DIA, "PAGO", 100.0, 2.15), (date(2026, 3, 17), "PAGO", 50.0, 1.20)],
            ),
        )
    )
    assert enmedio in dias
    assert dias[enmedio]["cuadra"] is False


def test_sin_ninguna_liquidacion_no_se_recorta_nada(spark):
    """El caso degenerado: si no ha llegado un solo fichero, no hay ventana que
    aplicar y el informe tiene que ensenar los dias con ventas sin liquidar."""
    dias = por_dia(conciliar(pedidos_df(spark, [(1, DIA, 100.0)]), liquidaciones_df(spark, [])))
    assert DIA in dias
    assert dias[DIA]["cuadra"] is False
