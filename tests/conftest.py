"""Fixtures compartidas y la red de seguridad del CI.

La sesion de Spark se crea una sola vez para toda la suite: arrancar una JVM
por test hace que los tests tarden minutos en vez de segundos.

Aqui vive tambien el mecanismo que impide que el CI mienta. Ver mas abajo.
"""

from __future__ import annotations

import importlib.util
import os

import pytest

EXIGE_PYSPARK = os.getenv("PRACTICA_EXIGE_PYSPARK") == "1"
"""¿Estamos donde saltarse los tests de Spark es inaceptable?

Los `pytest.importorskip("pyspark")` repartidos por la suite significan dos
cosas opuestas segun donde se ejecuten:

  * en tu portatil son una comodidad: lanzas `pytest` en el host, no tienes
    PySpark instalado y en un segundo ves los tests de logica pura,
  * en el CI son una MENTIRA. Un skip de coleccion no cambia el codigo de
    salida: pytest termina en 0, el check sale verde y todo parece correcto
    habiendo ejecutado 20 de los 97 tests. Un CI que da confianza falsa es peor
    que no tener CI, porque nadie vuelve a mirar.

La misma linea de codigo, con significados contrarios. Se resuelve con una
variable de entorno que solo pone el workflow.
"""


def pytest_configure(config: pytest.Config) -> None:
    """Falla ANTES de recolectar nada.

    Esta es la capa que atrapa el caso real. Los `importorskip` de los modulos
    se disparan al importarlos, o sea antes de que exista ningun test al que
    poder marcar como fallido: cuando eso pasa ya no hay a quien culpar, solo un
    contador mas bajo que nadie mira.
    """
    del config
    if not EXIGE_PYSPARK:
        return

    if importlib.util.find_spec("pyspark") is None:
        raise pytest.UsageError(
            "PRACTICA_EXIGE_PYSPARK=1 pero pyspark no esta instalado. "
            "Los tests deben correr dentro de la imagen de Glue "
            "(public.ecr.aws/glue/aws-glue-libs:5)."
        )

    # Iceberg no se detecta importando nada: son JAR en el disco de la imagen.
    # Sin esta comprobacion, test_silver_merge reventaria con un
    # ClassNotFoundException a mitad de la suite, en vez de aqui, en el segundo
    # cero y con un mensaje que dice lo que pasa.
    from common.spark_session import local_jars, running_on_glue

    if not running_on_glue() and not local_jars():
        raise pytest.UsageError(
            "Faltan los JAR de Iceberg (/usr/share/aws/iceberg/lib). "
            "Este runtime no es el de Glue 5.0: los tests de Iceberg fallarian "
            "o, peor, se saltarian."
        )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Segunda capa: con EXIGE_PYSPARK, cualquier skip individual pasa a fallo.

    No hay falsos positivos al usar `-m "not integration"`: un marcador
    DESELECCIONA los tests, no los salta, y un test deseleccionado no genera
    informe.
    """
    del item, call
    resultado = yield
    if not EXIGE_PYSPARK:
        return
    informe = resultado.get_result()
    if informe.skipped:
        informe.outcome = "failed"
        informe.longrepr = f"Skip prohibido cuando PRACTICA_EXIGE_PYSPARK=1: {informe.longrepr}"


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
