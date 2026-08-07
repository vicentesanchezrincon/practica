"""Acceso al Postgres de origen desde un job de Glue.

Las credenciales nunca estan en el codigo ni en los argumentos del job: se leen
de Secrets Manager en tiempo de ejecucion, usando solo el NOMBRE del secreto.
"""

from __future__ import annotations

import json
from functools import lru_cache

import boto3

DRIVER = "org.postgresql.Driver"


@lru_cache(maxsize=4)
def read_secret(secret_id: str) -> str:
    """Contenido del secreto, cacheado.

    Devuelve texto y no un dict porque lru_cache exige que el resultado sea
    hashable. Usa `credentials()` para obtener el dict ya parseado.
    """
    client = boto3.client("secretsmanager")
    return client.get_secret_value(SecretId=secret_id)["SecretString"]


def credentials(secret_id: str) -> dict:
    return json.loads(read_secret(secret_id))


def jdbc_url(secret: dict, db_name: str) -> str:
    # sslmode=require: el trafico va cifrado aunque no salga de la VPC.
    return f"jdbc:postgresql://{secret['host']}:{secret['port']}/{db_name}?sslmode=require"


def connection_options(secret: dict, db_name: str) -> dict[str, str]:
    """Opciones que espera el lector/escritor JDBC de Spark."""
    return {
        "url": jdbc_url(secret, db_name),
        "user": secret["username"],
        "password": secret["password"],
        "driver": DRIVER,
        # Sin esto, escribir una columna UUID desde Spark falla con
        # "column is of type uuid but expression is of type character varying".
        #
        # Spark no tiene tipo UUID: lo lee y lo escribe como texto. Por defecto
        # el driver de Postgres declara los parametros de texto como VARCHAR y
        # se niega a convertirlos, aunque el valor sea un UUID perfectamente
        # valido. Con `unspecified` deja que el servidor infiera el tipo por el
        # destino, que es lo que hace psql y lo que uno esperaria.
        #
        # Es de esos ajustes que no se descubren leyendo: aparecen la primera
        # vez que una tabla usa un tipo que Spark no modela, y hasta entonces
        # todo funciona.
        "stringtype": "unspecified",
    }


def execute_sql(spark, options: dict[str, str], sql: str) -> None:
    """Ejecuta SQL arbitrario contra Postgres.

    Spark sabe leer y escribir tablas, pero no ejecutar DDL ni sentencias
    sueltas. Bajamos al driver JDBC de Java a traves de la JVM que Spark ya
    tiene levantada, lo que evita instalar psycopg en el runtime de Glue.
    """
    jvm = spark.sparkContext._jvm
    jvm.Class.forName(DRIVER)
    conn = jvm.java.sql.DriverManager.getConnection(
        options["url"], options["user"], options["password"]
    )
    try:
        stmt = conn.createStatement()
        try:
            stmt.execute(sql)
        finally:
            stmt.close()
    finally:
        conn.close()


def scalar_query(spark, options: dict[str, str], sql: str) -> dict:
    """Ejecuta una consulta que devuelve UNA fila y la trae como dict.

    Se usa para preguntar al origen cosas como cuantas filas hay pendientes o
    en que rango de IDs estan, antes de decidir como paralelizar la lectura.
    """
    row = (
        spark.read.format("jdbc")
        .options(**options)
        .option("dbtable", f"({sql}) AS q")
        .load()
        .collect()[0]
    )
    return row.asDict()
