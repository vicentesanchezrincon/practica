"""Exporta el Postgres LOCAL a Parquet en disco, listo para subir a S3.

Primera mitad de la siembra del RDS. La segunda la hace `src/jobs/seed_rds.py`,
que corre dentro de AWS y lee lo que acabe en S3.

Por que este rodeo: el RDS vive en subredes aisladas, sin ruta a internet. No
puedes hacerle un `psql` ni un `pg_dump` desde tu maquina. S3 es el unico punto
de encuentro entre tu portatil y la VPC.

    tu portatil                                    AWS
    ┌────────────┐   este script   ┌──────┐  aws s3 sync   ┌────┐  job  ┌─────┐
    │ Postgres   │ ──────────────► │ data │ ─────────────► │ S3 │ ────► │ RDS │
    │ (docker)   │                 │ /_seed│                └────┘       └─────┘
    └────────────┘                 └──────┘

Escribe en local y no directamente a S3 a proposito: hacer que Spark hable s3a
dentro del contenedor exige cuadrar a mano el classpath de hadoop-aws con el
del SDK de AWS, y no compensa para una utilidad de un solo uso. El `aws` CLI
del host sube los ficheros mucho mejor.

`make export-seed` encadena las dos cosas.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

from common.config import get_table, tablas_de
from common.spark_session import build_session, jdbc_options_from_env

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
log = logging.getLogger("export")

FILAS_POR_FICHERO = 500_000


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)  # fmt: skip
    p.add_argument("--output", default="data/_seed", help="directorio de salida")
    p.add_argument(
        "--tables",
        default="",
        help="lista separada por comas. Por defecto, todas las de INGESTION_ORDER. "
        "Util para reexportar solo los eventos, que son las que mas pesan",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    destino = Path(args.output).resolve()

    # Empezar de cero: si una tabla desaparece del origen, no queremos que se
    # quede un Parquet viejo suelto que luego el job cargue sin darse cuenta.
    if destino.exists():
        shutil.rmtree(destino)
    destino.mkdir(parents=True)

    spark = build_session("export-seed")
    jdbc = jdbc_options_from_env()
    log.info("Origen:  %s", jdbc["url"])
    log.info("Destino: %s", destino)

    # Solo las tablas que viven en Postgres. La de liquidaciones tambien esta
    # en INGESTION_ORDER, pero no sale de la base de datos: llega como ficheros
    # y se sube con `make upload-landing`. Se filtra por el TIPO de origen y no
    # por una lista de excepciones, para que una fuente nueva no obligue a
    # acordarse de este fichero.
    tablas = [t.strip() for t in args.tables.split(",") if t.strip()] or tablas_de("jdbc")

    total = 0
    for table in tablas:
        # El esquema sale de la propia tabla: los eventos viven en `analytics`
        # y las cuatro maestras en `ecommerce`. Con un `--schema` global habria
        # que ejecutar el script dos veces.
        spec = get_table(table)
        df = (
            spark.read.format("jdbc")
            .options(**jdbc)
            .option("dbtable", f"{spec.source.schema}.{table}")
            .load()
        )
        n = df.count()
        # Un fichero por tabla se lee mucho mas rapido que doscientos diminutos,
        # pero coalesce(1) obliga a que todo pase por un solo ejecutor. Con los
        # eventos, que son ordenes de magnitud mas filas, eso se atraganta: por
        # encima del umbral se dejan varias particiones.
        salida = df.coalesce(1) if n <= FILAS_POR_FICHERO else df.repartition(8)
        salida.write.mode("overwrite").parquet(f"file://{destino}/{table}")
        log.info("%-12s  %8d filas", table, n)
        total += n

    log.info("Exportadas %d filas. Ahora: make export-seed (o aws s3 sync).", total)
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("La exportacion ha fallado")
        sys.exit(1)
