"""Fixtures compartidas.

La sesion de Spark se crea una sola vez para toda la suite: arrancar una JVM
por test hace que los tests tarden minutos en vez de segundos.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    """SparkSession local con Iceberg, apuntando a un warehouse temporal."""
    pyspark = pytest.importorskip("pyspark", reason="Ejecuta los tests dentro del contenedor Glue")
    del pyspark

    import tempfile

    from common.spark_session import build_session

    with tempfile.TemporaryDirectory(prefix="practica-test-warehouse-") as warehouse:
        session = build_session("practica-tests", warehouse=f"file://{warehouse}")
        yield session
        session.stop()
