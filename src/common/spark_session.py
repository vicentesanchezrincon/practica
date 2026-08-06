"""Construccion de la sesion Spark, con Iceberg configurado.

El mismo codigo tiene que funcionar en dos sitios:

  * en el contenedor Glue local, donde no hay Glue Data Catalog y el warehouse
    es un directorio del disco,
  * en un job de AWS Glue real, donde el catalogo es el Glue Data Catalog y el
    warehouse esta en S3.

`build_session()` detecta donde esta y configura lo que toca. Asi los jobs no
tienen ni un `if` de entorno dentro de la logica de negocio.
"""

from __future__ import annotations

import glob
import os
import sys

from pyspark.sql import SparkSession

CATALOG = "glue_catalog"
"""Nombre del catalogo Iceberg. Se usa igual en local y en AWS:
las consultas quedan como  SELECT * FROM glue_catalog.practica_dev_silver.customers"""

LOCAL_JAR_GLOBS = (
    "/usr/share/aws/iceberg/lib/iceberg-spark-runtime-*.jar",
    "/usr/share/aws/glue-pds/jars/postgresql-*.jar",
)
"""La imagen de Glue trae estos JAR, pero fuera del classpath de Spark.

En un job real los anade la propia plataforma (--datalake-formats iceberg para
Iceberg, y el driver JDBC lo aporta la Glue Connection). En el contenedor local
no hay nadie que lo haga, asi que los buscamos y los anadimos nosotros.
Sin esto, el primer CREATE TABLE ... USING iceberg falla con ClassNotFoundException.
"""


def running_on_glue() -> bool:
    """¿Estamos dentro de un job de AWS Glue?

    Se mira `sys.argv`, no el entorno: Glue siempre inyecta `--JOB_NAME` en la
    linea de comandos del script. La deteccion por variables de entorno no vale
    (`GLUE_INSTALLATION_PATH` no existe en Glue 5.0), y ejecutar `pytest` en el
    contenedor local nunca pasa ese argumento.

    Esto no es cosmetico: si falla, los jobs escriben tablas Iceberg con el
    catalogo Hadoop en lugar del Glue Data Catalog. Los datos quedan bien en S3
    pero **no se registran en ningun sitio**, asi que Athena no las ve y todo
    parece funcionar hasta que alguien intenta consultarlas.
    """
    return "--JOB_NAME" in sys.argv or os.getenv("GLUE_INSTALLATION_PATH") is not None


def local_jars() -> list[str]:
    """JAR que hay que anadir al classpath cuando corremos fuera de AWS."""
    found = []
    for pattern in LOCAL_JAR_GLOBS:
        found.extend(sorted(glob.glob(pattern)))
    return found


def iceberg_conf(warehouse: str, *, on_glue: bool) -> dict[str, str]:
    """Configuracion de Spark para que Iceberg funcione.

    En un job de Glue esto mismo se pasa como parametros del job
    (--datalake-formats iceberg y los --conf), pero dejarlo aqui permite
    ejecutar los mismos jobs en local sin tocar nada.
    """
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG}.warehouse": warehouse,
        # Iceberg escribe ficheros nuevos en cada commit; sin esto se acumulan
        # metadatos indefinidamente y las lecturas se degradan.
        f"spark.sql.catalog.{CATALOG}.write.metadata.delete-after-commit.enabled": "true",
        f"spark.sql.catalog.{CATALOG}.write.metadata.previous-versions-max": "20",
    }

    if on_glue:
        conf.update(
            {
                f"spark.sql.catalog.{CATALOG}.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                f"spark.sql.catalog.{CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            }
        )
    else:
        # En local no hay Glue Data Catalog: Iceberg guarda los metadatos en el
        # propio directorio del warehouse.
        conf[f"spark.sql.catalog.{CATALOG}.type"] = "hadoop"

    return conf


def build_session(
    app_name: str,
    warehouse: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Devuelve una SparkSession lista para leer y escribir Iceberg.

    Args:
        app_name: nombre que aparece en la Spark UI.
        warehouse: raiz del warehouse Iceberg. En local, por defecto
            ./spark-warehouse; en Glue hay que pasar la ruta S3.
        extra_conf: configuracion adicional que sobrescribe la de por defecto.
    """
    on_glue = running_on_glue()
    warehouse = warehouse or os.getenv("ICEBERG_WAREHOUSE") or "file:///tmp/practica-warehouse"

    builder = SparkSession.builder.appName(app_name)

    for key, value in iceberg_conf(warehouse, on_glue=on_glue).items():
        builder = builder.config(key, value)

    if not on_glue:
        builder = (
            builder.master(os.getenv("SPARK_MASTER", "local[*]"))
            # En local no hay 200 particiones que repartir; con menos, los tests
            # y las pruebas manuales van mucho mas rapido.
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.session.timeZone", "UTC")
        )
        if jars := local_jars():
            builder = builder.config("spark.jars", ",".join(jars))

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    # Se deja constancia en los logs: el sintoma de equivocarse de catalogo es
    # que todo funciona pero las tablas no aparecen en Athena, y sin esta linea
    # no hay forma de saberlo mirando la ejecucion.
    print(
        f"[spark] catalogo={'GlueCatalog' if on_glue else 'hadoop'} warehouse={warehouse}",
        flush=True,
    )
    return spark


def jdbc_url(host: str, port: int | str, database: str) -> str:
    return f"jdbc:postgresql://{host}:{port}/{database}"


def jdbc_options_from_env() -> dict[str, str]:
    """Opciones JDBC contra el Postgres local, leidas del entorno del compose.

    En AWS estas credenciales NO vienen de variables de entorno: vienen de
    Secrets Manager a traves de la Glue Connection. Esto es solo para local.
    """
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "ecommerce")
    return {
        "url": jdbc_url(host, port, database),
        "user": os.getenv("POSTGRES_USER", "practica"),
        "password": os.getenv("POSTGRES_PASSWORD", "practica_local_only"),
        "driver": "org.postgresql.Driver",
    }
