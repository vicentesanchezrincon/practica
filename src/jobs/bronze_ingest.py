"""Capa Bronze: extraccion incremental del Postgres de origen a S3.

    RDS Postgres ──JDBC──► s3://<bucket>/bronze/ecommerce/<tabla>/ingestion_date=YYYY-MM-DD/

Regla de oro de Bronze: **no se limpia nada**. Ni se deduplica, ni se castea,
ni se descartan filas malas. Bronze es una copia fiel e inmutable de lo que
habia en el origen. Si manana cambias una regla de negocio, reprocesas Silver
desde aqui sin volver a tocar la base de datos de produccion.

Lo unico que se anade son columnas de linaje (`_ingested_at`, `_source_system`,
`_batch_id`), que responden a "¿de donde salio esta fila y cuando?".

Incremental por watermark: cada tabla recuerda hasta que `updated_at` llego la
ultima vez y solo pide lo posterior. Ver `common/watermark.py`.

Se lanza con:
    aws glue start-job-run --job-name practica-dev-bronze-ingest
    aws glue start-job-run --job-name practica-dev-bronze-ingest \\
        --arguments '{"--TABLES":"orders,order_items"}'
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import SOURCE_SYSTEM, get_table, tablas_de
from common.jdbc import connection_options, credentials, scalar_query
from common.watermark import WatermarkStore, format

# Cuantas filas queremos como mucho por cada hilo de lectura JDBC. Spark abre
# una conexion por particion; demasiadas ahogan al RDS, muy pocas hacen que un
# solo hilo cargue con todo.
ROWS_PER_PARTITION = 50_000
MAX_PARTITIONS = 8


def log(msg: str) -> None:
    print(f"[bronze] {msg}", flush=True)


def pending_stats(spark, options, spec, watermark) -> dict:
    """Cuantas filas hay pendientes y en que rango de la clave de particion.

    Se pregunta antes de leer para poder paralelizar bien: sin lowerBound y
    upperBound, Spark no puede repartir la lectura entre varios hilos.
    """
    origen = spec.source
    sql = f"""
        SELECT count(*) AS n,
               min({origen.partition_column}) AS lo,
               max({origen.partition_column}) AS hi,
               max({origen.watermark_column}) AS max_wm
        FROM {origen.schema}.{spec.name}
        WHERE {origen.watermark_column} > TIMESTAMP '{format(watermark)}'
    """
    return scalar_query(spark, options, sql)


def limite(valor) -> str:
    """Formatea un extremo del rango para `lowerBound`/`upperBound` de Spark.

    Spark trocea rangos numericos y tambien temporales, que es lo unico posible
    cuando la clave del origen es un UUID. Pero el limite viaja como cadena y
    lo parsea el driver: un `str(datetime)` de Python produce
    '2026-03-15 12:00:00+00:00', con dos puntos en el huso, que el parser de
    timestamps de Spark no acepta. Hay que darle formato ISO explicito.
    """
    if isinstance(valor, datetime):
        return valor.strftime("%Y-%m-%d %H:%M:%S.%f")
    return str(valor)


def read_incremental(spark, options, spec, watermark, stats) -> DataFrame:
    """Lee del origen solo lo que cambio desde el watermark."""
    origen = spec.source
    subquery = f"""
        (SELECT * FROM {origen.schema}.{spec.name}
         WHERE {origen.watermark_column} > TIMESTAMP '{format(watermark)}') AS t
    """

    reader = spark.read.format("jdbc").options(**options).option("dbtable", subquery)

    n_particiones = min(MAX_PARTITIONS, max(1, stats["n"] // ROWS_PER_PARTITION))
    if n_particiones > 1 and origen.partition_column and stats["lo"] is not None:
        # Spark trocea el rango [lo, hi] y lanza una consulta por trozo.
        reader = (
            reader.option("partitionColumn", origen.partition_column)
            .option("lowerBound", limite(stats["lo"]))
            .option("upperBound", limite(stats["hi"]))
            .option("numPartitions", str(n_particiones))
        )
    log(f"  leyendo con {n_particiones} particion(es)")
    return reader.load()


def add_lineage(
    df: DataFrame, batch_id: str, ingested_at: datetime, system: str = SOURCE_SYSTEM
) -> DataFrame:
    """Anade el rastro de por donde paso la fila.

    Cuando dentro de seis meses alguien pregunte "¿de donde sale este dato?",
    estas cuatro columnas son la respuesta. `system` sale del origen y no de una
    constante del modulo: en cuanto el lake tiene datos de dos sitios, saber de
    cual vino cada fila deja de ser una curiosidad.
    """
    return (
        df.withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("_source_system", F.lit(system))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("ingestion_date", F.lit(ingested_at.date().isoformat()))
    )


def lectura_desde(spec, watermark: datetime) -> datetime:
    """Desde donde se lee de verdad: el watermark menos la ventana de reproceso.

    Sin ventana, cualquier fila que llegue al origen con una marca anterior a
    la ultima leida es invisible para siempre. No falla nada: simplemente no
    esta, y el hueco solo se nota cuando alguien cuadra los totales meses
    despues.

    A cambio se releen filas ya ingestadas. Es barato: Bronze es append-only a
    proposito y Silver deduplica. La asimetria es toda la justificacion de la
    ventana, releer cuesta unos segundos y perder datos cuesta una auditoria.
    """
    return watermark - spec.source.lookback


def ingest_table(spark, options, store, table, bucket, batch_id, ingested_at) -> dict:
    spec = get_table(table)
    watermark = store.read(table)
    desde = lectura_desde(spec, watermark)
    if desde != watermark:
        log(
            f"{table}: desde {format(desde)} (watermark {format(watermark)} "
            f"- ventana de {spec.source.lookback})"
        )
    else:
        log(f"{table}: desde {format(watermark)}")

    stats = pending_stats(spark, options, spec, desde)
    pendientes = stats["n"]

    if pendientes == 0:
        log(f"{table}: sin cambios")
        return {"tabla": table, "filas": 0, "watermark": format(watermark)}

    df = read_incremental(spark, options, spec, desde, stats)
    df = add_lineage(df, batch_id, ingested_at, spec.source.system)

    destino = f"s3://{bucket}/bronze/{spec.bronze_path_suffix}"
    # append y no overwrite: Bronze es un registro historico. Si un reintento
    # vuelve a traer las mismas filas, se duplican aqui a proposito y Silver las
    # deduplica. Perder filas seria mucho peor que repetirlas.
    df.write.mode("append").partitionBy("ingestion_date").parquet(destino)

    # El watermark avanza hasta el ultimo updated_at que hemos leido de verdad,
    # no hasta "ahora". Usar la hora actual dejaria fuera cualquier fila que se
    # confirmara en el origen mientras el job estaba leyendo.
    nuevo = stats["max_wm"]
    store.write(table, nuevo)

    log(f"{table}: {pendientes} filas -> {destino}")
    return {"tabla": table, "filas": pendientes, "watermark": format(nuevo)}


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "ENVIRONMENT", "BUCKET", "SECRET_ID", "DB_NAME"],
    )
    # --TABLES es opcional: sin el se ingestan todas las de JDBC, en el orden
    # declarado. Solo las de JDBC: los ficheros del proveedor de pagos tienen
    # su propio job, porque no hay watermark que consultar ni consulta que
    # trocear. Preguntar por el TIPO y no llevar una lista de excepciones es lo
    # que evita que una fuente nueva rompa este job sin que nadie lo toque.
    tables = tablas_de("jdbc")
    if "--TABLES" in sys.argv:
        extra = getResolvedOptions(sys.argv, ["TABLES"])
        tables = [t.strip() for t in extra["TABLES"].split(",") if t.strip()]

    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # El id del run de Glue como identificador de lote: permite rastrear
    # cualquier fila de S3 hasta la ejecucion concreta que la trajo.
    batch_id = args.get("JOB_RUN_ID") or spark.sparkContext.applicationId
    ingested_at = datetime.now(UTC)

    secret = credentials(args["SECRET_ID"])
    options = connection_options(secret, args["DB_NAME"])
    store = WatermarkStore(args["ENVIRONMENT"])

    log(f"lote {batch_id}, tablas: {', '.join(tables)}")

    resumen = [
        ingest_table(spark, options, store, table, args["BUCKET"], batch_id, ingested_at)
        for table in tables
    ]

    total = sum(r["filas"] for r in resumen)
    log("--- resumen ---")
    for r in resumen:
        log(f"  {r['tabla']:<12} {r['filas']:>8} filas   watermark={r['watermark']}")
    log(f"total: {total} filas")

    spark.stop()


if __name__ == "__main__":
    main()
