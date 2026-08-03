"""Job de Glue que siembra el RDS: aplica el DDL y carga los datos desde S3.

Segunda mitad de la siembra. La primera es `data_generator/export_to_s3.py`,
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

import json
import sys

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# Orden de carga: los padres antes que los hijos.
TABLES = ["customers", "products", "orders", "order_items"]

# Columnas BIGSERIAL cuya secuencia hay que recolocar despues de la carga.
SEQUENCES = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}

SCHEMA = "ecommerce"


def log(msg: str) -> None:
    print(f"[seed-rds] {msg}", flush=True)


def read_secret(secret_id: str) -> dict:
    """Usuario, contrasena y endpoint del RDS. Nunca viajan en el codigo."""
    client = boto3.client("secretsmanager")
    return json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])


def read_s3_text(uri: str) -> str:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
    return body.read().decode("utf-8")


def jdbc_url(secret: dict, db_name: str) -> str:
    # sslmode=require: el trafico va cifrado aunque no salga de la VPC.
    return f"jdbc:postgresql://{secret['host']}:{secret['port']}/{db_name}?sslmode=require"


def execute_sql(spark: SparkSession, url: str, secret: dict, sql: str) -> None:
    """Ejecuta SQL arbitrario contra Postgres.

    Spark solo sabe leer y escribir tablas, no ejecutar DDL. Bajamos al driver
    JDBC de Java a traves de la JVM que Spark ya tiene levantada, lo que evita
    tener que instalar psycopg en el runtime de Glue.

    El driver de Postgres maneja bien varias sentencias separadas por ';' y el
    entrecomillado con dolares ($$) de las funciones, asi que el schema.sql
    entero se manda de una vez.
    """
    jvm = spark.sparkContext._jvm
    jvm.Class.forName("org.postgresql.Driver")
    conn = jvm.java.sql.DriverManager.getConnection(url, secret["username"], secret["password"])
    try:
        stmt = conn.createStatement()
        try:
            stmt.execute(sql)
        finally:
            stmt.close()
    finally:
        conn.close()


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "SECRET_ID", "SEED_PREFIX", "DB_NAME"])

    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    secret = read_secret(args["SECRET_ID"])
    url = jdbc_url(secret, args["DB_NAME"])
    props = {
        "user": secret["username"],
        "password": secret["password"],
        "driver": "org.postgresql.Driver",
    }
    log(f"RDS: {secret['host']}:{secret['port']}/{args['DB_NAME']}")

    # 1. Esquema. El DDL vive junto al codigo (data_generator/schema.sql) y se
    #    sube a S3 en cada despliegue, para no tener dos copias divergentes.
    bucket_root = args["SEED_PREFIX"].rsplit("/", 1)[0]
    ddl_uri = f"{bucket_root}/scripts/schema.sql"
    log(f"Aplicando DDL desde {ddl_uri}")
    execute_sql(spark, url, secret, read_s3_text(ddl_uri))

    # 2. Vaciado, para que el job sea idempotente.
    log("Vaciando tablas")
    execute_sql(
        spark,
        url,
        secret,
        f"TRUNCATE {', '.join(f'{SCHEMA}.{t}' for t in TABLES)} RESTART IDENTITY CASCADE",
    )

    # 3. Carga.
    total = 0
    for table in TABLES:
        origen = f"{args['SEED_PREFIX']}/{table}"
        df = spark.read.parquet(origen)
        n = df.count()
        (
            df.write.format("jdbc")
            .option("url", url)
            .option("dbtable", f"{SCHEMA}.{table}")
            # Sin batchsize, el driver hace un round-trip por fila y esto tarda
            # una eternidad.
            .option("batchsize", 5000)
            .options(**props)
            .mode("append")
            .save()
        )
        log(f"{table:<12} {n:>8} filas cargadas")
        total += n

    # 4. Recolocar las secuencias.
    #    Hemos insertado los IDs explicitamente, asi que las secuencias siguen
    #    en 1. Sin esto, el primer INSERT que haga Postgres por su cuenta
    #    chocaria con una clave primaria que ya existe.
    log("Recolocando secuencias")
    setvals = "; ".join(
        f"SELECT setval(pg_get_serial_sequence('{SCHEMA}.{t}', '{pk}'), "
        f"coalesce((SELECT max({pk}) FROM {SCHEMA}.{t}), 1))"
        for t, pk in SEQUENCES.items()
    )
    execute_sql(spark, url, secret, setvals)

    log(f"Listo: {total} filas en total.")
    spark.stop()


if __name__ == "__main__":
    main()
