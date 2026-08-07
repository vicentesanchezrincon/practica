"""Job de Glue que siembra el RDS: aplica el DDL y carga los datos desde S3.

Segunda mitad de la siembra. La primera es `data_generator/export_seed.py`,
que deja el Postgres local volcado en Parquet dentro del bucket.

Este job corre DENTRO de la VPC (gracias a la Glue Connection), que es la unica
forma de alcanzar un RDS en subredes aisladas.

    S3 (_seed/*.parquet) ──este job──► RDS Postgres

Es idempotente: trunca las tablas antes de cargar, asi que puedes relanzarlo
las veces que quieras.

Se lanza con:
    aws glue start-job-run --job-name practica-dev-seed-rds
"""

from __future__ import annotations

import sys

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

from common.config import get_table, tablas_de
from common.jdbc import connection_options, credentials, execute_sql
from common.watermark import WatermarkStore

# La lista de tablas y su orden salen del registro del pipeline, no de una
# copia local. Antes habia aqui una segunda lista escrita a mano: sobrevivio
# mientras las tablas fueron siempre las mismas cuatro, y anadir una quinta la
# habria dejado fuera de la siembra sin que nada fallara.
#
# Solo las de JDBC: este job siembra una base de datos, y los ficheros del
# proveedor de pagos llegan por su cuenta a la zona de aterrizaje.
TABLES = tablas_de("jdbc")

# Columnas BIGSERIAL cuya secuencia hay que recolocar despues de la carga.
# No todas las tablas tienen: los eventos se identifican con un UUID que genera
# el cliente, asi que no hay ninguna secuencia que dejar descolocada.
SEQUENCES = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}


def cualificada(table: str) -> str:
    """`esquema.tabla`. El esquema sale del origen declarado, porque ya no hay
    uno solo: los eventos viven en `analytics` y el resto en `ecommerce`."""
    return f"{get_table(table).source.schema}.{table}"


def log(msg: str) -> None:
    print(f"[seed-rds] {msg}", flush=True)


def read_s3_text(uri: str) -> str:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
    return body.read().decode("utf-8")


def main() -> None:
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "ENVIRONMENT", "SECRET_ID", "SEED_PREFIX", "DB_NAME"]
    )

    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    secret = credentials(args["SECRET_ID"])
    options = connection_options(secret, args["DB_NAME"])
    log(f"RDS: {secret['host']}:{secret['port']}/{args['DB_NAME']}")

    # 1. Esquema. El DDL vive junto al codigo (data_generator/schema.sql) y se
    #    sube a S3 en cada despliegue, para no tener dos copias divergentes.
    bucket_root = args["SEED_PREFIX"].rsplit("/", 1)[0]
    ddl_uri = f"{bucket_root}/scripts/schema.sql"
    log(f"Aplicando DDL desde {ddl_uri}")
    execute_sql(spark, options, read_s3_text(ddl_uri))

    # 2. Vaciado, para que el job sea idempotente.
    log("Vaciando tablas")
    execute_sql(
        spark,
        options,
        f"TRUNCATE {', '.join(cualificada(t) for t in TABLES)} RESTART IDENTITY CASCADE",
    )

    # 3. Carga.
    total = 0
    cargadas: dict[str, int] = {}
    for table in TABLES:
        origen = f"{args['SEED_PREFIX']}/{table}"
        df = spark.read.parquet(origen)
        n = df.count()
        (
            df.write.format("jdbc")
            .options(**options)
            .option("dbtable", cualificada(table))
            # Sin batchsize, el driver hace un round-trip por fila y esto tarda
            # una eternidad.
            .option("batchsize", 5000)
            .mode("append")
            .save()
        )
        log(f"{table:<12} {n:>8} filas cargadas")
        cargadas[table] = n
        total += n

    # 4. Recolocar las secuencias.
    #    Hemos insertado los IDs explicitamente, asi que las secuencias siguen
    #    en 1. Sin esto, el primer INSERT que haga Postgres por su cuenta
    #    chocaria con una clave primaria que ya existe.
    log("Recolocando secuencias")
    setvals = "; ".join(
        f"SELECT setval(pg_get_serial_sequence('{cualificada(t)}', '{pk}'), "
        f"coalesce((SELECT max({pk}) FROM {cualificada(t)}), 1))"
        for t, pk in SEQUENCES.items()
    )
    execute_sql(spark, options, setvals)

    # 5. Verificacion: releer el estado final desde el RDS.
    #
    #    No basta con contar lo que hemos escrito. Si el TRUNCATE del paso 2
    #    fallara, este job seguiria escribiendo el mismo numero de filas y el
    #    log se veria identico, pero la tabla tendria el doble. La unica prueba
    #    de que el job es idempotente es preguntarle a la base de datos cuantas
    #    filas hay DESPUES.
    log("Verificando contra el RDS")
    problemas = []
    for table in TABLES:
        escritas = cargadas[table]
        en_destino = (
            spark.read.format("jdbc")
            .options(**options)
            .option("dbtable", f"(SELECT count(*) AS n FROM {cualificada(table)}) AS t")
            .load()
            .collect()[0]["n"]
        )
        marca = "OK " if en_destino == escritas else "MAL"
        log(f"  {marca} {table:<12} escritas={escritas:>7}  en_rds={en_destino:>7}")
        if en_destino != escritas:
            problemas.append(f"{table}: escritas {escritas}, en RDS {en_destino}")

    if problemas:
        raise RuntimeError("El RDS no coincide con lo cargado: " + "; ".join(problemas))

    # 6. Invalidar los watermarks.
    #
    #    Este job acaba de TRUNCAR el origen y volver a cargarlo. Cualquier
    #    marca anterior dice "ya lei hasta aqui" sobre unos datos que ya no
    #    existen, y los nuevos suelen tener fechas mas antiguas: la extraccion
    #    incremental los da por vistos y NO LOS LEE NUNCA.
    #
    #    No falla nada. Bronze informa de "sin cambios", igual que un dia
    #    tranquilo, y el pipeline sigue corriendo sobre datos viejos. Se
    #    descubrio cuadrando la conciliacion: habia dias con cobros del
    #    proveedor y cero pedidos, porque las liquidaciones eran nuevas y los
    #    pedidos de Silver eran del lote anterior.
    #
    #    Lo hace el job y no el Makefile a proposito: quien invalida el estado
    #    debe ser quien lo rompe, no quien se acuerde de llamarlo despues.
    borradas = WatermarkStore(args["ENVIRONMENT"]).reset(TABLES)
    log(f"Watermarks invalidados: {', '.join(borradas) if borradas else 'no habia ninguno'}")

    log(f"Listo: {total} filas en total, verificadas contra el RDS.")
    spark.stop()


if __name__ == "__main__":
    main()
