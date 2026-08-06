"""Tests de la maquina de estados.

Se comprueba la definicion ASL que genera el CDK. Lo que se fija son los
errores que solo darian la cara ejecutando, cuando ya has gastado tiempo y
dinero:

  * lanzar los jobs sin esperarlos, y que Silver empiece con Bronze a medias,
  * publicar en SNS un objeto donde se espera un string,
  * quedarse sin `Catch` y que un fallo pase desapercibido,
  * dejar la ejecucion programada encendida en dev.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Template

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent / "src"))

from stacks.database_stack import DatabaseStack  # noqa: E402
from stacks.glue_stack import GlueStack  # noqa: E402
from stacks.network_stack import NetworkStack  # noqa: E402
from stacks.orchestration_stack import OrchestrationStack  # noqa: E402
from stacks.storage_stack import StorageStack  # noqa: E402

ENV = cdk.Environment(account="123456789012", region="eu-west-1")


def build(environment: str = "dev", alert_email: str | None = None):
    app = cdk.App()
    network = NetworkStack(app, "T-Network", environment=environment, env=ENV)
    storage = StorageStack(app, "T-Storage", environment=environment, env=ENV)
    database = DatabaseStack(
        app,
        "T-Database",
        environment=environment,
        env=ENV,
        vpc=network.vpc,
        rds_security_group=network.rds_sg,
        glue_security_group=network.glue_sg,
    )
    glue_stack = GlueStack(
        app,
        "T-Glue",
        environment=environment,
        env=ENV,
        bucket=storage.bucket,
        secret=database.secret,
        secret_name=database.secret_name,
        connection_name=database.connection_name,
        glue_security_group=network.glue_sg,
    )
    orchestration = OrchestrationStack(
        app,
        "T-Orchestration",
        environment=environment,
        env=ENV,
        bucket=storage.bucket,
        bronze_job=glue_stack.bronze_job,
        silver_job=glue_stack.silver_job,
        gold_job=glue_stack.gold_job,
        alert_email=alert_email,
    )
    return Template.from_stack(orchestration)


@pytest.fixture(scope="module")
def dev():
    return build("dev")


@pytest.fixture(scope="module")
def prod():
    return build("prod")


def definicion(template: Template) -> str:
    """La definicion ASL como texto plano, lista para buscar dentro.

    El CDK la genera como un Fn::Join con las comillas escapadas; aqui se
    aplanan una sola vez para que los tests no tengan que acordarse.
    """
    maquinas = template.find_resources("AWS::StepFunctions::StateMachine")
    bruto = json.dumps(next(iter(maquinas.values()))["Properties"]["DefinitionString"])
    return bruto.replace("\\", "")


# ------------------------------------------------------------- estructura ---


def test_se_crea_una_unica_maquina_de_estados(dev):
    dev.resource_count_is("AWS::StepFunctions::StateMachine", 1)


def test_los_tres_jobs_estan_en_el_pipeline(dev):
    asl = definicion(dev)
    for estado in ("Bronze", "Silver", "Gold"):
        assert f'"{estado}"' in asl


def test_el_orden_es_bronze_silver_gold(dev):
    """Silver lee lo que Bronze escribio y Gold lo que Silver dejo. Ejecutarlos
    en otro orden produce resultados incompletos sin dar ningun error."""
    asl = definicion(dev)
    assert '"Bronze":{"Next":"Silver"' in asl
    assert '"Silver":{"Next":"LeerInformeDeCalidad"' in asl


# ------------------------------------------------------------------ sync ---


def test_los_jobs_se_esperan_a_terminar(dev):
    """La integracion .sync es lo que hace que la maquina espere. Sin ella,
    Silver arrancaria con Bronze a medias y leeria datos incompletos."""
    asl = definicion(dev)
    assert "glue:startJobRun.sync" in asl
    assert 'glue:startJobRun"' not in asl  # la version que no espera


def test_los_jobs_reintentan_con_backoff(dev):
    asl = definicion(dev)
    assert '"ErrorEquals":["States.TaskFailed"]' in asl
    assert '"BackoffRate":2' in asl


# -------------------------------------------------------- gate de calidad ---


def test_la_calidad_se_lee_de_s3_sin_lambda(dev):
    """Una Lambda solo para leer un JSON de S3 seria una pieza mas que mantener,
    desplegar y vigilar. La integracion SDK lo resuelve sin nada de eso."""
    dev.resource_count_is("AWS::Lambda::Function", 0)
    assert "s3:getObject" in definicion(dev)


def test_el_informe_se_parsea_antes_de_consultarlo(dev):
    """S3 devuelve el cuerpo como texto. Sin StringToJson, el Choice no puede
    mirar dentro y la condicion nunca se cumpliria."""
    assert "States.StringToJson" in definicion(dev)


def test_gold_solo_se_construye_si_la_calidad_pasa(dev):
    asl = definicion(dev)
    assert '"Variable":"$.calidad.informe.passed"' in asl
    assert '"BooleanEquals":true' in asl


def test_la_parada_por_calidad_se_distingue_de_un_fallo(dev):
    """Son cosas distintas: una es el pipeline funcionando como debe ante datos
    malos; la otra es que algo se rompio. Mezclarlas hace imposible saber que
    esta pasando mirando las alertas."""
    asl = definicion(dev)
    assert '"Error":"CalidadInsuficiente"' in asl
    assert '"Error":"PipelineFallido"' in asl


# ---------------------------------------------------------------- alertas ---


def test_los_mensajes_de_sns_se_serializan_a_texto(dev):
    """SNS exige que Message sea un string. Pasar un objeto falla en ejecucion,
    no en el synth: es de los errores que solo aparecen desplegando."""
    asl = definicion(dev)
    assert "States.JsonToString" in asl
    assert '"Message.$":"$.error"' not in asl


def test_todos_los_pasos_tienen_catch(dev):
    """Sin Catch, un job que revienta deja la ejecucion en rojo y nadie se
    entera hasta que alguien mira la consola por casualidad."""
    asl = definicion(dev)
    assert asl.count('"ErrorEquals":["States.ALL"]') >= 4


def test_sin_correo_configurado_no_hay_suscripcion(dev):
    """El topic se crea igual: asi se puede suscribir despues sin redesplegar."""
    dev.resource_count_is("AWS::SNS::Topic", 1)
    dev.resource_count_is("AWS::SNS::Subscription", 0)


def test_con_correo_configurado_se_suscribe():
    con_correo = build("dev", alert_email="alguien@example.com")
    con_correo.has_resource_properties(
        "AWS::SNS::Subscription", {"Protocol": "email", "Endpoint": "alguien@example.com"}
    )


# -------------------------------------------------------------- programado ---


def test_la_ejecucion_programada_esta_apagada_en_dev(dev):
    """Que se despierte de madrugada contra una infraestructura ya destruida
    solo genera alertas de fallo inutiles."""
    dev.has_resource_properties("AWS::Events::Rule", {"State": "DISABLED"})


def test_la_ejecucion_programada_esta_encendida_en_prod(prod):
    prod.has_resource_properties("AWS::Events::Rule", {"State": "ENABLED"})


def test_la_maquina_tiene_timeout(dev):
    """Sin timeout, una ejecucion colgada consume hasta que alguien la mata."""
    assert '"TimeoutSeconds":7200' in definicion(dev)
