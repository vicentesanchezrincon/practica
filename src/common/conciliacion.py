"""Conciliacion: cuadrar lo que dice tu sistema con lo que dice un tercero.

Es la unica metrica del proyecto que **cruza dos fuentes de origen distinto**:
los pedidos vienen de Postgres y las liquidaciones de un fichero del proveedor
de pagos. Y es la unica que puede detectar un fallo que ninguna de las dos ve
por separado, porque cada una es internamente coherente.

La identidad que tiene que cumplirse:

    importe_liquidado = importe_pedidos - devoluciones - comisiones

Nunca cuadra exactamente, y eso es normal: el proveedor cobra una comision, hay
devoluciones que se liquidan dias despues que su venta, y hay redondeos. Lo que
importa no es que el descuadre sea cero, es que **se mantenga dentro de un
margen y que se sepa a que se debe**.

Un descuadre creciente casi nunca significa "hay un error de calculo". Suele
significar una de estas cuatro cosas, todas silenciosas:

  * un fichero que no llego y nadie echo de menos,
  * un fichero procesado dos veces,
  * la referencia del proveedor cruzada sin normalizar, que no casa ni una fila
    y produce una conciliacion vacia que parece que no hubo ventas,
  * o las fechas leidas con el formato equivocado, que descoloca los importes
    de dia sin cambiar ningun total.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TOLERANCIA = 0.02
"""Descuadre relativo admitido, sobre el importe de los pedidos del dia.

Un 2% cubre el desfase normal entre la fecha de un pedido y la de su
liquidacion. Poner 0 seria garantizar una alarma diaria y que la senal acabe
ignorada, que es la peor forma de perder una comprobacion.
"""


def conciliar(pedidos: DataFrame, liquidaciones: DataFrame) -> DataFrame:
    """Un dia por fila: lo cobrado segun nosotros, segun el proveedor, y la diferencia.

    Se agregan las DOS partes por separado antes de juntarlas, y no se hace un
    join fila a fila. Un pedido puede tener varios movimientos —un pago y su
    devolucion— y un movimiento puede no tener pedido —un ajuste del proveedor—,
    asi que cruzarlos al detalle multiplicaria filas y el total saldria inflado
    sin que nada fallara.

    El join es FULL OUTER a proposito: hace falta ver los dias que estan en un
    lado y no en el otro. Un dia con ventas y sin liquidacion es exactamente el
    fichero que no llego, y con un `inner` desapareceria del informe.
    """
    por_pedidos = pedidos.groupBy(F.col("order_date").alias("dia")).agg(
        F.round(F.sum("total_amount"), 2).alias("importe_pedidos"),
        F.countDistinct("order_id").alias("pedidos"),
    )

    por_liquidacion = liquidaciones.groupBy(F.col("fecha_operacion").alias("dia")).agg(
        F.round(F.sum(F.when(F.col("tipo") == "PAGO", F.col("importe"))), 2).alias("cobros"),
        F.round(F.sum(F.when(F.col("tipo") == "DEVOL", -F.col("importe"))), 2).alias(
            "devoluciones"
        ),
        F.round(F.sum("comision"), 2).alias("comisiones"),
        F.count("*").alias("movimientos"),
    )

    unidos = por_pedidos.join(por_liquidacion, on="dia", how="full_outer")

    # Los ceros son deliberados y no cosmetica: con nulos, cualquier resta
    # posterior se propaga a NULL y el dia problematico DESAPARECE del informe,
    # que es justo el dia que hay que mirar.
    cero = [
        "importe_pedidos",
        "cobros",
        "devoluciones",
        "comisiones",
        "pedidos",
        "movimientos",
    ]
    for columna in cero:
        unidos = unidos.withColumn(columna, F.coalesce(F.col(columna), F.lit(0.0)))

    return (
        unidos.withColumn("importe_liquidado", F.round(F.col("cobros") - F.col("devoluciones"), 2))
        .withColumn(
            # Lo que deberia acabar en el banco: ventas menos devoluciones y
            # menos lo que se queda el proveedor. Es informativo; NO es lo que
            # se compara.
            "esperado",
            F.round(F.col("importe_pedidos") - F.col("devoluciones") - F.col("comisiones"), 2),
        )
        .withColumn(
            # El cuadre es en BRUTO: los cobros del dia contra las ventas del
            # dia. Devoluciones y comisiones quedan fuera, y no por simplificar:
            # una devolucion casi nunca cae el mismo dia que su venta, y la
            # comision es el precio del servicio, no dinero que falte.
            # Metiendolas, TODOS los dias saldrian descuadrados por motivos
            # normales y la senal dejaria de servir para lo unico que importa:
            # detectar dinero que falta o que sobra.
            "descuadre",
            F.round(F.col("cobros") - F.col("importe_pedidos"), 2),
        )
        .withColumn(
            "descuadre_relativo",
            F.when(F.col("importe_pedidos") == 0, F.lit(None).cast("double")).otherwise(
                F.round(F.abs(F.col("descuadre")) / F.col("importe_pedidos"), 4)
            ),
        )
        .withColumn(
            "cuadra",
            # Un dia sin pedidos pero CON liquidacion no cuadra, aunque el
            # descuadre relativo no se pueda calcular: es dinero que llega de
            # ninguna parte.
            F.when(F.col("importe_pedidos") == 0, F.col("importe_liquidado") == 0).otherwise(
                F.col("descuadre_relativo") <= F.lit(TOLERANCIA)
            ),
        )
    )


def resumen(conciliacion: DataFrame) -> dict:
    """Cifras para la puerta de calidad. Un dict, no un DataFrame.

    Lo consume el informe JSON que lee Step Functions, y ahi hace falta un
    valor, no una tabla.
    """
    fila = conciliacion.agg(
        F.count("*").alias("dias"),
        F.sum(F.when(~F.col("cuadra"), 1).otherwise(0)).alias("dias_descuadrados"),
        F.round(F.sum("importe_pedidos"), 2).alias("importe_pedidos"),
        F.round(F.sum("importe_liquidado"), 2).alias("importe_liquidado"),
        F.round(F.sum("comisiones"), 2).alias("comisiones"),
        F.round(F.max("descuadre_relativo"), 4).alias("peor_descuadre"),
    ).collect()[0]

    return {
        "dias": fila["dias"],
        "dias_descuadrados": int(fila["dias_descuadrados"] or 0),
        "importe_pedidos": fila["importe_pedidos"],
        "importe_liquidado": fila["importe_liquidado"],
        "comisiones": fila["comisiones"],
        "peor_descuadre": fila["peor_descuadre"],
        "tolerancia": TOLERANCIA,
    }
