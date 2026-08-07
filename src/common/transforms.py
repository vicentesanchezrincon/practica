"""Transformaciones reutilizables de la capa Silver.

Dos operaciones, y las dos son declarativas: leen que hacer de `TableSpec`, asi
que anadir una tabla al pipeline no obliga a escribir codigo de Spark nuevo.

  * `normalize`  arregla lo que tiene arreglo (espacios, mayusculas)
  * `deduplicate` se queda con la version mas reciente de cada registro
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common.config import LINEAGE_PREFIX, TableSpec

ROW_NUMBER_COLUMN = "_rn"


def normalize(df: DataFrame, spec: TableSpec) -> DataFrame:
    """Limpia lo que es ruido de formato, no un dato malo.

    El orden importa: esto va ANTES de validar. Un email con espacios sobrantes
    y mayusculas es un email valido mal escrito, y mandarlo a cuarentena seria
    tirar un cliente real a la basura por un problema de tecleo.

    Lo que NO se arregla aqui son los datos que estan mal de verdad: un
    'ESP' donde se esperaba 'ES' no se corrige adivinando, se manda a cuarentena.
    """
    columnas = set(df.columns)

    for columna in spec.lower_trim:
        if columna in columnas:
            df = df.withColumn(columna, F.lower(F.trim(F.col(columna))))

    for columna in spec.upper_trim:
        if columna in columnas:
            df = df.withColumn(columna, F.upper(F.trim(F.col(columna))))

    return df


def deduplicate(df: DataFrame, spec: TableSpec) -> DataFrame:
    """Deja una fila por clave de negocio: la mas reciente.

    Hacen falta dos motivos distintos:

      1. El origen tiene duplicados de verdad (el generador los inyecta a
         proposito, y en la vida real los hay).
      2. Bronze es append-only: si un job se reintenta, las mismas filas
         aparecen dos veces, y si una fila se modifico varias veces entre dos
         ingestas, hay varias versiones de la misma clave.

    Se ordena por `spec.dedup_order` descendente, y se desempata por
    `_ingested_at`: si dos filas empatan en todo (duplicado exacto del origen),
    nos quedamos con la que entro mas tarde. Sin ese desempate el resultado
    dependeria del orden en que Spark leyera los ficheros, y el job dejaria de
    ser determinista.

    **El criterio se declara por tabla y no se deduce.** Con origenes JDBC la
    respuesta parecia obvia —gana el `updated_at` mas alto— hasta el punto de
    estar escrita a fuego aqui. Pero eso solo vale si el origen actualiza filas.
    En uno append-only no hay ninguna columna que signifique "esta version
    sustituye a aquella", y elegir la equivocada no da error: descarta datos
    buenos en silencio.
    """
    if not spec.dedup_order:
        raise ValueError(
            f"{spec.name} no declara dedup_order: no hay forma de saber que fila "
            f"debe sobrevivir cuando una clave de negocio aparece repetida."
        )

    orden = [F.col(columna).desc() for columna in spec.dedup_order]
    if f"{LINEAGE_PREFIX}ingested_at" in df.columns:
        orden.append(F.col(f"{LINEAGE_PREFIX}ingested_at").desc())

    ventana = Window.partitionBy(*[F.col(k) for k in spec.business_key]).orderBy(*orden)

    return (
        df.withColumn(ROW_NUMBER_COLUMN, F.row_number().over(ventana))
        .filter(F.col(ROW_NUMBER_COLUMN) == 1)
        .drop(ROW_NUMBER_COLUMN)
    )


def drop_bronze_only_columns(df: DataFrame) -> DataFrame:
    """Quita las columnas que solo tienen sentido en Bronze.

    `ingestion_date` es la particion fisica de Bronze, no un atributo del
    negocio: en Silver una fila existe una sola vez, no una por dia de ingesta.
    El resto del linaje (_ingested_at, _batch_id, _source_system) si se conserva
    para poder rastrear el origen de cada fila.
    """
    return df.drop("ingestion_date") if "ingestion_date" in df.columns else df
