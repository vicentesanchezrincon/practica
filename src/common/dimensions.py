"""Piezas del modelado dimensional: claves subrogadas, miembro desconocido y SCD2.

Tres ideas que conviene tener claras antes de leer el codigo:

**Clave subrogada.** Las tablas de hechos no apuntan a la clave del origen
(`customer_id`) sino a una clave propia del almacen (`customer_key`). Hace falta
porque con SCD2 un mismo `customer_id` tiene VARIAS filas en la dimension, una
por version, y el hecho tiene que apuntar a una concreta.

Aqui las claves se calculan con un hash determinista de la clave de negocio (mas
`valid_from` en las dimensiones historicas) en vez de con un contador. Un
contador (`monotonically_increasing_id`) daria valores distintos en cada
ejecucion y el pipeline dejaria de ser idempotente.

**Miembro desconocido.** La fila `-1` de cada dimension. Existe porque un hecho
puede referirse a algo que no esta en la dimension: en este proyecto pasa de
verdad, porque Silver no es referencialmente cerrada (un cliente en cuarentena
no llega a Silver, pero sus pedidos si). Descartar esos hechos falsearia los
ingresos; mandarlos al miembro desconocido los conserva y deja el problema
visible.

**SCD tipo 2.** Cuando un atributo de una dimension cambia, no se sobrescribe:
se cierra la version anterior y se abre una nueva. Asi un pedido de marzo se
puede analizar con el segmento que el cliente tenia en marzo, no con el de hoy.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

UNKNOWN_KEY = -1
"""Clave del miembro desconocido. Negativa a proposito: nunca colisiona con un
hash y se ve a simple vista en una consulta."""

FAR_FUTURE = datetime(9999, 12, 31, tzinfo=UTC)
"""Fin de validez de la version vigente.

Se usa una fecha centinela en vez de NULL para que el join temporal se pueda
escribir como `fecha >= valid_from AND fecha < valid_to` sin tener que anadir
un `OR valid_to IS NULL` que ademas impide usar indices y particiones."""


def surrogate_key(*columns: str | Column) -> Column:
    """Clave subrogada determinista a partir de las columnas dadas.

    Determinista es la palabra importante: la misma entrada produce siempre la
    misma clave, asi que reconstruir Gold desde cero no invalida nada de lo
    construido antes. Con un contador incremental, cada reconstruccion
    reasignaria claves distintas y los hechos antiguos apuntarian a la fila
    equivocada.

    Se fuerza a positivo porque las claves negativas estan reservadas para los
    miembros desconocidos.
    """
    cols = [F.col(c) if isinstance(c, str) else c for c in columns]
    return F.abs(F.xxhash64(*cols))


def unknown_member(df: DataFrame, key_column: str, overrides: dict[str, Column] | None = None):
    """Fila `-1` con el mismo esquema que la dimension.

    Se construye a partir del esquema del propio DataFrame para que no haya que
    mantener una lista de columnas en paralelo: si manana la dimension gana una
    columna, el miembro desconocido la gana tambien.
    """
    overrides = overrides or {}
    valores = []
    for campo in df.schema.fields:
        if campo.name == key_column:
            valores.append(F.lit(UNKNOWN_KEY).cast(campo.dataType).alias(campo.name))
        elif campo.name in overrides:
            valores.append(overrides[campo.name].cast(campo.dataType).alias(campo.name))
        elif campo.dataType.simpleString() == "string":
            # Un texto legible es mucho mas util que un NULL cuando alguien
            # consulta y se encuentra con el miembro desconocido.
            valores.append(F.lit("(desconocido)").alias(campo.name))
        else:
            valores.append(F.lit(None).cast(campo.dataType).alias(campo.name))

    return df.sparkSession.range(1).select(*valores)


def as_of_join(
    facts: DataFrame,
    dimension: DataFrame,
    *,
    natural_key: str,
    fact_date: str,
    key_column: str,
) -> DataFrame:
    """Une un hecho con la version de la dimension vigente en su fecha.

    Esto es para lo que existe el SCD tipo 2. Un join normal por clave natural
    devolveria varias filas (una por version) y multiplicaria los hechos; un
    join contra `is_current` daria el estado de HOY, que es justo lo que no
    quieres al analizar el pasado.

    Los hechos que no encuentran version van al miembro desconocido en lugar de
    perderse.
    """
    dim = dimension.select(
        F.col(natural_key).alias("_nk"),
        F.col(key_column).alias("_sk"),
        F.col("valid_from").alias("_desde"),
        F.col("valid_to").alias("_hasta"),
    )

    condicion = (
        (facts[natural_key] == dim["_nk"])
        & (facts[fact_date] >= dim["_desde"])
        & (facts[fact_date] < dim["_hasta"])
    )

    return (
        facts.join(dim, condicion, how="left")
        .withColumn(key_column, F.coalesce(F.col("_sk"), F.lit(UNKNOWN_KEY)))
        .drop("_nk", "_sk", "_desde", "_hasta")
    )


def scd2_changes(
    incoming: DataFrame,
    current: DataFrame,
    *,
    natural_key: str,
    tracked: list[str],
    effective_from: str,
) -> tuple[DataFrame, DataFrame]:
    """Compara el estado nuevo con las versiones vigentes de la dimension.

    Devuelve (versiones_a_cerrar, versiones_a_abrir).

    Ojo con la comparacion de atributos: hay que usar `<=>` (null-safe) y no
    `!=`. En SQL, `NULL != 'gold'` es NULL, o sea ni verdadero ni falso, asi que
    un cambio desde NULL pasaria desapercibido y la dimension se quedaria sin
    esa version. Es un bug clasico y silencioso.
    """
    vigentes = current.filter(F.col("is_current")).select(
        F.col(natural_key).alias("_nk"),
        *[F.col(c).alias(f"_actual_{c}") for c in tracked],
    )

    comparado = incoming.join(vigentes, incoming[natural_key] == F.col("_nk"), how="left")

    es_nuevo = F.col("_nk").isNull()
    # <=> es el igual null-safe de Spark. Un simple != daria NULL al comparar
    # contra NULL, y el cambio pasaria desapercibido.
    alguno_cambio = F.expr(" OR ".join(f"NOT ({c} <=> _actual_{c})" for c in tracked))
    ha_cambiado = ~es_nuevo & alguno_cambio

    limpiar = ["_nk", *[f"_actual_{c}" for c in tracked]]

    a_cerrar = comparado.filter(ha_cambiado).select(
        F.col(natural_key), F.col(effective_from).alias("_cierre")
    )
    a_abrir = comparado.filter(es_nuevo | ha_cambiado).drop(*limpiar)

    return a_cerrar, a_abrir
