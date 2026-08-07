"""Sesionizacion: reconstruir las visitas a partir de eventos sueltos.

El origen manda un `session_id` en cada evento, y es tentador agrupar por el y
dar el trabajo por hecho. No sirve: ese identificador vive en una cookie, y una
cookie dura mucho mas que una visita. Alguien que deja la pestana abierta,
come, y vuelve por la tarde manda las dos visitas con el MISMO session_id.

Agrupar por el identificador del cliente produce sesiones de seis horas con una
pausa de cinco en medio. No falla nada: la duracion media de sesion sale
disparatada, la tasa de rebote sale bajisima, y los numeros son creibles como
para que nadie los mire dos veces.

La sesion se **deriva del tiempo**: eventos del mismo visitante separados por
mas de un hueco de inactividad son visitas distintas. Es el mismo patron que
en el sector energetico separa dos periodos de funcionamiento de una maquina
cuando la telemetria se interrumpe.
"""

from __future__ import annotations

from datetime import timedelta

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

HUECO_POR_DEFECTO = timedelta(minutes=30)
"""Inactividad a partir de la cual empieza otra visita.

Treinta minutos es la convencion del sector (la heredo Google Analytics y se
quedo). No es una verdad: es un acuerdo. Lo importante es que este declarado en
un sitio y que quien lea la cifra sepa cual se uso, porque cambiarlo cambia
todas las metricas de sesion a la vez.
"""


def marcar_sesiones(
    eventos: DataFrame,
    *,
    hueco: timedelta = HUECO_POR_DEFECTO,
    clave_visitante: str = "session_id",
    columna_tiempo: str = "event_time",
) -> DataFrame:
    """Anade `session_key`: el identificador de la visita DERIVADA.

    Tres pasos, el clasico "islas y huecos":

      1. para cada evento, cuanto tiempo paso desde el anterior del mismo
         visitante (`lag` sobre una ventana ordenada por tiempo);
      2. marcar con un 1 los eventos que abren visita (los que superan el
         hueco, y el primero de cada visitante, cuyo `lag` es nulo);
      3. sumar acumuladamente esas marcas: el resultado es un contador que solo
         avanza al empezar una visita nueva, y por tanto la identifica.

    Se ordena por `event_time` y no por `received_at` a proposito: la visita la
    define cuando el visitante hizo las cosas, no cuando nos llegaron.
    """
    por_visitante = Window.partitionBy(clave_visitante).orderBy(columna_tiempo)

    anterior = F.lag(F.col(columna_tiempo)).over(por_visitante)
    segundos_desde_el_anterior = F.col(columna_tiempo).cast("long") - anterior.cast("long")

    abre_visita = F.when(
        anterior.isNull() | (segundos_desde_el_anterior > int(hueco.total_seconds())), 1
    ).otherwise(0)

    acumulado = por_visitante.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    return eventos.withColumn(
        "session_key",
        F.concat_ws("#", F.col(clave_visitante), F.sum(abre_visita).over(acumulado)),
    )


def _tuvo(tipo: str) -> Column:
    """Si la visita contiene al menos un evento de ese tipo."""
    return F.max(F.when(F.col("event_type") == tipo, 1).otherwise(0)) == 1


def resumir_sesiones(eventos: DataFrame, *, hueco: timedelta = HUECO_POR_DEFECTO) -> DataFrame:
    """Una fila por visita, con su recorrido por el embudo.

    El grano es la visita derivada, no el `session_id` del cliente. `visitas`
    en la salida no existe: lo que se cuenta aqui son visitas, y cada una vale
    uno. Si hiciera falta comparar con lo que declaraba el origen, esa es una
    consulta aparte y deliberada, no un numero mezclado con los demas.

    `anonima` se conserva porque no es lo mismo "no sabemos quien era" que
    "fue un cliente concreto": en Gold, las anonimas caen en el miembro
    desconocido en vez de perderse en un join.
    """
    con_visita = marcar_sesiones(eventos, hueco=hueco)

    return con_visita.groupBy("session_key").agg(
        F.min("event_time").alias("session_start"),
        F.max("event_time").alias("session_end"),
        (F.max("event_time").cast("long") - F.min("event_time").cast("long")).alias(
            "duracion_segundos"
        ),
        F.count("*").alias("eventos"),
        # El cliente puede identificarse a mitad de la visita: empieza anonimo y
        # hace login. `max` se queda con el id en cuanto aparece, que es lo que
        # se quiere; con `first` dependeria del orden y saldria nulo la mitad
        # de las veces.
        F.max("customer_id").alias("customer_id"),
        F.max("customer_id").isNull().alias("anonima"),
        F.first("device", ignorenulls=True).alias("device"),
        # La atribucion es del PRIMER contacto de la visita: la campana que la
        # trajo, no la ultima pagina que se vio.
        F.first("utm_source", ignorenulls=True).alias("utm_source"),
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias(
            "productos_vistos"
        ),
        _tuvo("add_to_cart").alias("anadio_al_carrito"),
        _tuvo("checkout_start").alias("inicio_compra"),
        _tuvo("purchase").alias("compro"),
        F.coalesce(
            F.sum(F.when(F.col("event_type") == "purchase", F.col("amount"))), F.lit(0.0)
        ).alias("importe"),
    )
