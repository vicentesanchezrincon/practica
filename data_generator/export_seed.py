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

from common.config import INGESTION_ORDER
from common.spark_session import build_session, jdbc_options_from_env

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
log = logging.getLogger("export")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)  # fmt: skip
    p.add_argument("--output", default="data/_seed", help="directorio de salida")
    p.add_argument("--schema", default="ecommerce", help="esquema de origen en Postgres")
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

    total = 0
    for table in INGESTION_ORDER:
        df = (
            spark.read.format("jdbc")
            .options(**jdbc)
            .option("dbtable", f"{args.schema}.{table}")
            .load()
        )
        n = df.count()
        # coalesce(1): son volumenes pequenos y un fichero por tabla se lee
        # despues mucho mas rapido que doscientos ficheros diminutos.
        df.coalesce(1).write.mode("overwrite").parquet(f"file://{destino}/{table}")
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
