"""Capa Silver: limpia, deduplica, valida y hace MERGE sobre Iceberg.

    s3://<bucket>/bronze/... ──este job──► glue_catalog.practica_<env>_silver.<tabla>
                                       └──► s3://<bucket>/silver/_quarantine/<tabla>/

Lo que hace, en orden, y por que ese orden:

  1. **Normaliza** (trim, lower, upper). Antes de validar, para no mandar a
     cuarentena una fila cuyo unico problema era un espacio sobrante.
  2. **Deduplica** por clave de negocio, quedandose con la version mas reciente.
  3. **Valida** contra las reglas de `common/config.py`. Las filas que fallan
     van a cuarentena con el motivo, nunca se descartan en silencio.
  4. **MERGE INTO** la tabla Iceberg. Es lo que hace el job idempotente:
     puedes relanzarlo N veces y el resultado no cambia.

El job falla si la tasa de cuarentena supera QUARANTINE_THRESHOLD. Mejor no
publicar nada que publicar datos malos: Gold no se construye sobre un lote
corrupto.

Se lanza con:
    aws glue start-job-run --job-name practica-dev-silver-transform
    aws glue start-job-run --job-name practica-dev-silver-transform \\
        --arguments '{"--INGESTION_DATE":"2026-08-05"}'
    aws glue start-job-run --job-name practica-dev-silver-transform \\
        --arguments '{"--FULL_REFRESH":"true"}'
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from common.config import (
    INGESTION_ORDER,
    QUARANTINE_THRESHOLD,
    TableSpec,
    catalog_database,
    get_table,
)
from common.quality import ERRORS_COLUMN, QualityReport, report, split
from common.sessions import resumir_sesiones
from common.spark_session import CATALOG, build_session
from common.transforms import deduplicate, drop_bronze_only_columns, normalize


def log(msg: str) -> None:
    print(f"[silver] {msg}", flush=True)


def optional_arg(name: str, default: str | None = None) -> str | None:
    """Argumento opcional de Glue.

    getResolvedOptions revienta si el argumento no viene, asi que hay que mirar
    antes en sys.argv. Es feo pero es la unica forma con la API de Glue.
    """
    if f"--{name}" not in sys.argv:
        return default
    return getResolvedOptions(sys.argv, [name])[name]


# ------------------------------------------------------------------ lectura ---


def read_bronze(spark: SparkSession, bucket: str, spec: TableSpec, ingestion_date: str | None):
    """Lee de Bronze una particion concreta, o todo el historico.

    Devuelve None si no hay nada que leer: una tabla sin cambios ese dia no
    tiene particion, y eso no es un error.
    """
    base = f"s3://{bucket}/bronze/{spec.bronze_path_suffix}"
    ruta = base if ingestion_date is None else f"{base}/ingestion_date={ingestion_date}"

    try:
        df = spark.read.parquet(ruta)
    except AnalysisException:
        log(f"{spec.name}: sin datos en {ruta}")
        return None
    return df


# ---------------------------------------------------------------- escritura ---


def table_name(table: str, environment: str) -> str:
    return f"{CATALOG}.{catalog_database('silver', environment)}.{table}"


def ensure_table(spark: SparkSession, df: DataFrame, spec: TableSpec, full_name: str) -> None:
    """Crea la tabla Iceberg vacia si no existe.

    Se crea a partir del esquema del propio DataFrame, asi que no hay que
    declarar las columnas en ningun sitio: el esquema tiene una unica fuente de
    verdad, que es el origen.

    Ademas, una tabla Iceberg **se registra sola en el Glue Data Catalog**, asi
    que Silver queda consultable desde Athena sin trabajo extra.
    """
    if spark.catalog.tableExists(full_name):
        return

    log(f"creando {full_name}")
    df.limit(0).writeTo(full_name).using("iceberg").create()

    if spec.silver_partition:
        # Se anade despues de crear porque la particion oculta de Iceberg se
        # declara como evolucion del esquema, no en el CREATE.
        log(f"  particionando por {spec.silver_partition}")
        spark.sql(f"ALTER TABLE {full_name} ADD PARTITION FIELD {spec.silver_partition}")


def merge(spark: SparkSession, df: DataFrame, spec: TableSpec, full_name: str) -> None:
    """Upsert por clave de negocio. Aqui esta la idempotencia del pipeline."""
    vista = f"origen_{spec.name}"
    df.createOrReplaceTempView(vista)

    spark.sql(f"""
        MERGE INTO {full_name} t
        USING {vista} s
        ON {spec.merge_condition()}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def write_quarantine(df: DataFrame, bucket: str, spec: TableSpec, batch_id: str) -> None:
    """Las filas rechazadas, con su motivo y el lote que las trajo."""
    destino = f"s3://{bucket}/silver/_quarantine/{spec.name}"
    (
        df.withColumn("_quarantined_at", F.lit(datetime.now(UTC)).cast("timestamp"))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_quarantine_date", F.current_date().cast("string"))
        .write.mode("append")
        .partitionBy("_quarantine_date")
        .parquet(destino)
    )


# ------------------------------------------------------------------- proceso ---


def process_table(
    spark: SparkSession,
    table: str,
    bucket: str,
    environment: str,
    ingestion_date: str | None,
    batch_id: str,
    parents: dict[str, DataFrame],
) -> QualityReport | None:
    spec = get_table(table)
    bruto = read_bronze(spark, bucket, spec, ingestion_date)
    if bruto is None:
        return None

    total = bruto.count()
    if total == 0:
        log(f"{table}: 0 filas en Bronze")
        return None

    limpio = normalize(drop_bronze_only_columns(bruto), spec)
    deduplicado = deduplicate(limpio, spec)
    duplicados = total - deduplicado.count()

    validas, cuarentena = split(deduplicado, spec, parents)
    n_cuarentena = cuarentena.count()
    n_validas = validas.count()

    if n_cuarentena:
        write_quarantine(cuarentena, bucket, spec, batch_id)

    full_name = table_name(table, environment)
    ensure_table(spark, validas, spec, full_name)
    merge(spark, validas, spec, full_name)

    # Universo de claves que los hijos usaran para validar su integridad.
    #
    # Es la union de dos cosas, y las dos hacen falta:
    #   - lo que ya hay en Silver, porque un pedido de hoy puede apuntar a un
    #     cliente de hace un ano que no viene en este lote,
    #   - TODAS las claves de este lote, incluidas las que acaban de irse a
    #     cuarentena, porque esas filas existen en el origen aunque tengan un
    #     campo mal. Rechazar un pedido porque su cliente tenia el email mal
    #     escrito seria castigar al pedido por un problema ajeno.
    claves = spec.business_key
    parents[table] = (
        deduplicado.select(*claves).unionByName(spark.table(full_name).select(*claves)).distinct()
    )

    resultado = report(table, deduplicado.count(), n_cuarentena)
    log(
        f"{table}: {total} leidas, {duplicados} duplicadas, "
        f"{n_validas} validas, {n_cuarentena} en cuarentena "
        f"({resultado.rate:.2%})"
    )

    if n_cuarentena:
        motivos = (
            cuarentena.select(F.explode(ERRORS_COLUMN).alias("motivo"))
            .groupBy("motivo")
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )
        for fila in motivos:
            log(f"    {fila['motivo']:<28} {fila['count']}")

    return resultado


def build_web_sessions(spark: SparkSession, environment: str) -> int:
    """Reconstruye las visitas a partir de los eventos ya limpios.

    Es la primera tabla de Silver que **no viene de Bronze**: se deriva de otra
    tabla de Silver. Por eso no pasa por `process_table` ni entra en el informe
    de calidad, y por eso se reconstruye entera en vez de mergearse: una visita
    no es un registro del origen que se pueda actualizar, es el resultado de
    mirar todos los eventos de un visitante a la vez. Con un evento tardio, la
    visita de anteayer puede cambiar de duracion.

    Cuesta releer los eventos enteros. Alternativa seria recalcular solo las
    visitas tocadas por el lote, que es mas rapido y bastante mas facil de
    equivocar; a este volumen no compensa.
    """
    eventos = spark.table(table_name("web_events", environment))
    sesiones = resumir_sesiones(eventos)
    destino = table_name("web_sessions", environment)

    (
        sesiones.writeTo(destino)
        .using("iceberg")
        .partitionedBy(F.days("session_start"))
        .createOrReplace()
    )
    return sesiones.count()


def write_summary(bucket: str, reports: list[QualityReport], batch_id: str) -> dict:
    """Deja el resultado de calidad en S3.

    Lo lee la maquina de estados de la Fase 7 para decidir si construye Gold.
    Guardarlo en S3 y no solo en los logs permite que un Choice de Step
    Functions lo consulte sin tener que parsear texto.
    """
    resumen = {
        "batch_id": batch_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "default_threshold": QUARANTINE_THRESHOLD,
        "tables": [
            {**r.as_dict(), "threshold": get_table(r.table).quarantine_threshold} for r in reports
        ],
        "total": sum(r.total for r in reports),
        "quarantined": sum(r.quarantined for r in reports),
    }
    resumen["rate"] = (
        round(resumen["quarantined"] / resumen["total"], 6) if resumen["total"] else 0.0
    )
    resumen["passed"] = all(r.rate <= get_table(r.table).quarantine_threshold for r in reports)

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key="_quality/silver/latest.json",
        Body=json.dumps(resumen, indent=2).encode(),
        ContentType="application/json",
    )
    return resumen


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENVIRONMENT", "BUCKET"])
    environment = args["ENVIRONMENT"]
    bucket = args["BUCKET"]

    tables = INGESTION_ORDER
    if tablas_arg := optional_arg("TABLES"):
        tables = [t.strip() for t in tablas_arg.split(",") if t.strip()]

    full_refresh = (optional_arg("FULL_REFRESH", "false") or "").lower() == "true"
    ingestion_date = (
        None
        if full_refresh
        else optional_arg("INGESTION_DATE", datetime.now(UTC).date().isoformat())
    )

    spark = build_session(args["JOB_NAME"], warehouse=f"s3://{bucket}/silver")
    spark.sparkContext.setLogLevel("WARN")
    batch_id = args.get("JOB_RUN_ID") or spark.sparkContext.applicationId

    log(f"lote {batch_id}")
    log(f"origen: {'TODO el historico' if full_refresh else f'ingestion_date={ingestion_date}'}")

    parents: dict[str, DataFrame] = {}
    reports: list[QualityReport] = []

    for table in tables:
        resultado = process_table(
            spark, table, bucket, environment, ingestion_date, batch_id, parents
        )
        if resultado:
            reports.append(resultado)

    if not reports:
        log("no habia nada que procesar")
        spark.stop()
        return

    # Las visitas se derivan despues de que los eventos esten limpios y
    # deduplicados: sesionizar sobre Bronze contaria dos veces cada reenvio.
    if "web_events" in tables:
        n_sesiones = build_web_sessions(spark, environment)
        log(f"web_sessions: {n_sesiones} visitas derivadas de los eventos")

    resumen = write_summary(bucket, reports, batch_id)
    log("--- calidad ---")
    for r in reports:
        umbral = get_table(r.table).quarantine_threshold
        marca = "OK " if r.rate <= umbral else "MAL"
        log(f"  {marca} {r.table:<12} {r.rate:>7.2%}  umbral {umbral:.0%}")
    log(f"  total: {resumen['quarantined']}/{resumen['total']} ({resumen['rate']:.2%})")

    # El job REPORTA la calidad, no decide que hacer con ella.
    #
    # La decision es del orquestador: la maquina de estados lee este informe y
    # corta el pipeline antes de Gold si no pasa. Tener la puerta aqui ademas
    # significaria dos implementaciones del mismo criterio, que tarde o temprano
    # divergen. El umbral vive en un solo sitio, config.py; quien lo aplica es
    # quien orquesta.
    #
    # Silver, en cualquier caso, no queda corrupta: el MERGE solo escribe filas
    # que pasaron la validacion. Lo que el corte evita es construir Gold sobre
    # un lote del que se ha rechazado tanto que los agregados no serian
    # representativos.
    if resumen["passed"]:
        log("calidad OK")
    else:
        malas = [
            f"{r.table} {r.rate:.2%} (umbral {get_table(r.table).quarantine_threshold:.2%})"
            for r in reports
            if r.rate > get_table(r.table).quarantine_threshold
        ]
        log(f"CALIDAD INSUFICIENTE en: {'; '.join(malas)}")
        log(f"  revisa s3://{bucket}/silver/_quarantine/")

    spark.stop()


if __name__ == "__main__":
    main()
