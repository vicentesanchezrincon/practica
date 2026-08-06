"""Validacion de la capa Silver y separacion en cuarentena.

Principio: **las filas malas no se descartan en silencio**. Se apartan a una
tabla de cuarentena con el motivo concreto por el que fallaron, para que alguien
pueda mirarlas, arreglar el origen y reprocesar.

Descartar en silencio es la forma mas rapida de perder la confianza en un data
lake: los numeros no cuadran y nadie sabe por que.

Cada fila rechazada lleva `_quality_errors`, un array con todos sus problemas
(no solo el primero): si un pedido tiene el importe negativo Y un cliente
huerfano, quieres enterarte de las dos cosas a la vez.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from common.config import TableSpec

ERRORS_COLUMN = "_quality_errors"


@dataclass(frozen=True)
class QualityReport:
    table: str
    total: int
    valid: int
    quarantined: int

    @property
    def rate(self) -> float:
        """Proporcion de filas rechazadas, entre 0 y 1."""
        return self.quarantined / self.total if self.total else 0.0

    def as_dict(self) -> dict:
        return {
            "table": self.table,
            "total": self.total,
            "valid": self.valid,
            "quarantined": self.quarantined,
            "rate": round(self.rate, 6),
        }


def _rules(spec: TableSpec, df: DataFrame) -> list[tuple[Column, str]]:
    """Pares (condicion_de_fallo, motivo) derivados de la configuracion.

    Solo se aplican reglas sobre columnas que existan en el DataFrame: asi una
    tabla a la que todavia no le ha llegado una columna nueva no revienta.
    """
    columnas = set(df.columns)
    reglas: list[tuple[Column, str]] = []

    for columna in spec.not_null:
        if columna in columnas:
            reglas.append((F.col(columna).isNull(), f"{columna}_nulo"))

    for columna in spec.non_negative:
        if columna in columnas:
            reglas.append((F.col(columna) < 0, f"{columna}_negativo"))

    for columna, patron in spec.patterns.items():
        if columna in columnas:
            # isNull se excluye aqui: de eso ya se encarga la regla not_null, y
            # no queremos el mismo problema reportado dos veces.
            reglas.append(
                (F.col(columna).isNotNull() & ~F.col(columna).rlike(patron), f"{columna}_formato"),
            )

    return reglas


def _referential_rules(
    spec: TableSpec, df: DataFrame, parents: dict[str, DataFrame]
) -> tuple[DataFrame, list[tuple[Column, str]]]:
    """Comprueba que las claves foraneas apuntan a algo que EXISTIO en el origen.

    Ojo con contra que se compara: `parents` contiene el universo de claves
    vistas en el ORIGEN, no las filas que sobrevivieron a la validacion.

    La diferencia importa mucho. Validar contra Silver provoca un efecto domino:
    un cliente rechazado por tener el email mal deja huerfanos a todos sus
    pedidos, y esos pedidos dejan huerfanas a todas sus lineas. Medido con los
    datos de prueba, un 1% de huerfanos reales se convertia en un 10% de
    cuarentena, casi 7x de amplificacion, y la tasa dejaba de significar nada.

    Un pedido cuyo cliente existe pero esta mal escrito es un pedido correcto:
    el problema es del cliente y ya esta registrado en su propia cuarentena.
    """
    reglas: list[tuple[Column, str]] = []

    for columna, (tabla_padre, columna_padre) in spec.references.items():
        if columna not in df.columns or tabla_padre not in parents:
            continue

        marca = f"_existe_{columna}"
        padre = (
            parents[tabla_padre]
            .select(F.col(columna_padre).alias(f"_pk_{columna}"))
            .distinct()
            .withColumn(marca, F.lit(True))
        )
        df = df.join(padre, df[columna] == padre[f"_pk_{columna}"], how="left").drop(
            f"_pk_{columna}"
        )

        # Solo es huerfana si la clave tiene valor y no encuentra padre. Un NULL
        # ya lo captura la regla not_null.
        reglas.append((F.col(columna).isNotNull() & F.col(marca).isNull(), f"{columna}_huerfano"))

    return df, reglas


def split(
    df: DataFrame, spec: TableSpec, parents: dict[str, DataFrame] | None = None
) -> tuple[DataFrame, DataFrame]:
    """Parte el DataFrame en (validas, cuarentena).

    Las de cuarentena llevan `_quality_errors` con TODOS los motivos por los que
    fallaron, no solo el primero.
    """
    columnas_originales = df.columns
    df, referenciales = _referential_rules(spec, df, parents or {})
    reglas = _rules(spec, df) + referenciales

    if not reglas:
        vacio = df.limit(0).withColumn(ERRORS_COLUMN, F.array().cast("array<string>"))
        return df, vacio

    errores = F.array_compact(
        F.array(*[F.when(condicion, F.lit(motivo)) for condicion, motivo in reglas])
    )
    marcado = df.withColumn(ERRORS_COLUMN, errores).select(*columnas_originales, ERRORS_COLUMN)

    validas = marcado.filter(F.size(ERRORS_COLUMN) == 0).drop(ERRORS_COLUMN)
    cuarentena = marcado.filter(F.size(ERRORS_COLUMN) > 0)
    return validas, cuarentena


def report(table: str, total: int, quarantined: int) -> QualityReport:
    return QualityReport(
        table=table, total=total, valid=total - quarantined, quarantined=quarantined
    )
