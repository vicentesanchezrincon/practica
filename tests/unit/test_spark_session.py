"""Test de humo del entorno local.

Si esto pasa, tienes un Spark 3.5 con Iceberg funcionando dentro del
contenedor de Glue y puedes empezar a escribir jobs de verdad (Fase 4).

Se ejecuta con:  make test
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark", reason="Ejecuta los tests dentro del contenedor Glue: make test")

from common.spark_session import (  # noqa: E402
    CATALOG,
    iceberg_conf,
    jdbc_url,
    running_on_glue,
)


def test_se_detecta_glue_por_el_argumento_job_name(monkeypatch):
    """Glue siempre inyecta --JOB_NAME en la linea de comandos del script.

    Este test nacio de un fallo real: la deteccion miraba variables de entorno
    (`GLUE_INSTALLATION_PATH`, que no existe en Glue 5.0, y un `os.getenv`
    sobre `--JOB_NAME`, que ni siquiera es una variable). Nunca detectaba nada,
    asi que los jobs escribian tablas Iceberg con el catalogo Hadoop en vez del
    Glue Data Catalog: los datos quedaban bien en S3 pero sin registrar, y
    Athena no las veia. Todo "funcionaba" hasta que alguien consultaba.
    """
    monkeypatch.delenv("GLUE_INSTALLATION_PATH", raising=False)

    monkeypatch.setattr("sys.argv", ["script.py", "--JOB_NAME", "practica-dev-silver-transform"])
    assert running_on_glue() is True

    monkeypatch.setattr("sys.argv", ["pytest", "tests/unit"])
    assert running_on_glue() is False


def test_la_config_local_de_iceberg_usa_catalogo_hadoop():
    conf = iceberg_conf("file:///tmp/wh", on_glue=False)
    assert conf[f"spark.sql.catalog.{CATALOG}.type"] == "hadoop"
    assert f"spark.sql.catalog.{CATALOG}.catalog-impl" not in conf


def test_la_config_de_glue_usa_el_data_catalog():
    conf = iceberg_conf("s3://bucket/silver", on_glue=True)
    assert conf[f"spark.sql.catalog.{CATALOG}.catalog-impl"].endswith("GlueCatalog")
    assert conf[f"spark.sql.catalog.{CATALOG}.io-impl"].endswith("S3FileIO")
    assert f"spark.sql.catalog.{CATALOG}.type" not in conf


def test_la_url_jdbc_tiene_el_formato_de_postgres():
    assert jdbc_url("db", 5432, "ecommerce") == "jdbc:postgresql://db:5432/ecommerce"


def test_spark_arranca_y_cuenta_filas(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "letra"])
    assert df.count() == 2


def test_se_puede_crear_y_leer_una_tabla_iceberg(spark):
    """La prueba que de verdad importa: Iceberg esta en el classpath y commitea."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.humo")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.humo.ping")
    spark.sql(f"CREATE TABLE {CATALOG}.humo.ping (id BIGINT, nombre STRING) USING iceberg")
    spark.sql(f"INSERT INTO {CATALOG}.humo.ping VALUES (1, 'uno'), (2, 'dos')")

    assert spark.table(f"{CATALOG}.humo.ping").count() == 2

    # MERGE: es la operacion sobre la que se construye toda la capa Silver.
    spark.sql(
        f"""
        MERGE INTO {CATALOG}.humo.ping t
        USING (SELECT 2L AS id, 'DOS' AS nombre UNION ALL SELECT 3L, 'tres') s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.nombre = s.nombre
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    resultado = {r["id"]: r["nombre"] for r in spark.table(f"{CATALOG}.humo.ping").collect()}
    assert resultado == {1: "uno", 2: "DOS", 3: "tres"}
