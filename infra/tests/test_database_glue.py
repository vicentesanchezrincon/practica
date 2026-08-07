"""Tests del RDS, la Glue Connection y el rol/job de Glue.

Como en el resto de tests de infraestructura, se comprueba la plantilla de
CloudFormation, sin desplegar nada. Lo que se fija aqui son las decisiones que
duelen si alguien las cambia sin querer:

  * que el RDS nunca sea accesible desde internet,
  * que no haya ni una contrasena en texto plano en la plantilla,
  * que el job de Glue conserve la Connection (sin ella no ve el RDS),
  * que prod no se pueda destruir por accidente.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent / "src"))

from stacks.database_stack import DatabaseStack  # noqa: E402
from stacks.glue_stack import GlueStack  # noqa: E402
from stacks.network_stack import NetworkStack  # noqa: E402
from stacks.storage_stack import StorageStack  # noqa: E402

ENV = cdk.Environment(account="123456789012", region="eu-west-1")


def build_all(environment: str = "dev"):
    """Monta el conjunto completo, porque estos stacks se referencian entre si."""
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
    return Template.from_stack(database), Template.from_stack(glue_stack)


@pytest.fixture(scope="module")
def dev():
    return build_all("dev")


@pytest.fixture(scope="module")
def prod():
    return build_all("prod")


# -------------------------------------------------------------------- RDS ---


def test_el_rds_no_es_accesible_desde_internet(dev):
    database, _ = dev
    database.has_resource_properties("AWS::RDS::DBInstance", {"PubliclyAccessible": False})


def test_el_rds_esta_cifrado(dev):
    database, _ = dev
    database.has_resource_properties("AWS::RDS::DBInstance", {"StorageEncrypted": True})


def test_el_rds_usa_una_instancia_del_free_tier(dev):
    database, _ = dev
    database.has_resource_properties("AWS::RDS::DBInstance", {"DBInstanceClass": "db.t4g.micro"})


def test_la_contrasena_no_aparece_en_la_plantilla(dev):
    """La genera AWS y vive en Secrets Manager. Si alguien la escribe a mano en
    el codigo, acaba en CloudFormation, que cualquiera con acceso puede leer."""
    database, _ = dev
    texto = json.dumps(database.to_json())
    assert "MasterUserPassword" not in texto or "resolve:secretsmanager" in texto
    for sospechoso in ("practica_local_only", "password123", "postgres123"):
        assert sospechoso not in texto


def test_se_crea_el_secreto(dev):
    database, _ = dev
    database.resource_count_is("AWS::SecretsManager::Secret", 1)


def test_dev_se_puede_destruir_y_prod_no(dev, prod):
    database_dev, _ = dev
    database_prod, _ = prod
    database_dev.has_resource("AWS::RDS::DBInstance", {"DeletionPolicy": "Delete"})
    database_dev.has_resource_properties("AWS::RDS::DBInstance", {"DeletionProtection": False})
    database_prod.has_resource("AWS::RDS::DBInstance", {"DeletionPolicy": "Retain"})
    database_prod.has_resource_properties("AWS::RDS::DBInstance", {"DeletionProtection": True})


def test_prod_conserva_backups_y_dev_no(dev, prod):
    """En dev los backups solo alargan el destroy; en prod son imprescindibles."""
    database_dev, _ = dev
    database_prod, _ = prod
    database_dev.has_resource_properties("AWS::RDS::DBInstance", {"BackupRetentionPeriod": 0})
    database_prod.has_resource_properties("AWS::RDS::DBInstance", {"BackupRetentionPeriod": 7})


# ------------------------------------------------------------- connection ---


def test_la_connection_es_jdbc_y_usa_el_secreto(dev):
    database, _ = dev
    database.has_resource_properties(
        "AWS::Glue::Connection",
        {
            "ConnectionInput": Match.object_like(
                {
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": Match.object_like(
                        {"SECRET_ID": Match.any_value(), "JDBC_ENFORCE_SSL": "true"}
                    ),
                }
            )
        },
    )


def test_el_secret_id_es_un_nombre_literal(dev):
    """`secret.secret_name` del CDK genera una expresion que trocea el ARN
    partiendolo por guiones, y solo funciona mientras el nombre no lleve
    ninguno. Aqui se fija el nombre a mano para no depender de esa casualidad."""
    database, _ = dev
    conexiones = database.find_resources("AWS::Glue::Connection")
    secret_id = next(iter(conexiones.values()))["Properties"]["ConnectionInput"][
        "ConnectionProperties"
    ]["SECRET_ID"]
    assert secret_id == "practica/dev/postgres", f"deberia ser literal, es {secret_id!r}"


def test_la_connection_declara_subred_y_security_group(dev):
    """Sin PhysicalConnectionRequirements, Glue no entra en la VPC y el job no
    ve el RDS por mucho que la Connection exista."""
    database, _ = dev
    conexiones = database.find_resources("AWS::Glue::Connection")
    requisitos = next(iter(conexiones.values()))["Properties"]["ConnectionInput"][
        "PhysicalConnectionRequirements"
    ]
    assert requisitos["SubnetId"]
    assert requisitos["SecurityGroupIdList"]
    assert requisitos["AvailabilityZone"]


# -------------------------------------------------------------- glue job ---


def test_el_job_usa_la_connection(dev):
    """Es lo que lo mete dentro de la VPC. Sin esto falla por timeout."""
    _, glue_stack = dev
    glue_stack.has_resource_properties(
        "AWS::Glue::Job", {"Connections": {"Connections": ["practica-dev-postgres"]}}
    )


def test_solo_los_jobs_que_hablan_con_el_rds_llevan_connection(dev):
    """Meter un job en la VPC cuando no lo necesita solo anade formas de
    fallar: ENIs que crear, endpoints de los que depender y arranques mas
    lentos. Silver solo lee S3 y el catalogo, asi que corre fuera."""
    _, glue_stack = dev
    con_connection = set()
    sin_connection = set()
    for job in glue_stack.find_resources("AWS::Glue::Job").values():
        nombre = job["Properties"]["Name"]
        if job["Properties"].get("Connections"):
            con_connection.add(nombre)
        else:
            sin_connection.add(nombre)

    assert "practica-dev-seed-rds" in con_connection
    assert "practica-dev-bronze-ingest" in con_connection
    assert "practica-dev-silver-transform" in sin_connection
    # El de ficheros tampoco: que el origen sea de un tercero no significa que
    # haya que meterlo en la VPC. Los ficheros ya estan en el bucket.
    assert "practica-dev-bronze-files" in sin_connection
    assert "practica-dev-gold-build" in sin_connection


def test_los_jobs_de_iceberg_declaran_datalake_formats(dev):
    """Silver y Gold escriben Iceberg. Sin --datalake-formats, los JAR no estan
    en el classpath y el primer CREATE TABLE ... USING iceberg falla."""
    _, glue_stack = dev
    jobs = {
        j["Properties"]["Name"]: j["Properties"]["DefaultArguments"]
        for j in glue_stack.find_resources("AWS::Glue::Job").values()
    }
    # bronze-files tambien escribe Iceberg: su registro de control de ficheros
    # procesados es una tabla Iceberg.
    for nombre in (
        "practica-dev-silver-transform",
        "practica-dev-gold-build",
        "practica-dev-bronze-files",
    ):
        assert jobs[nombre].get("--datalake-formats") == "iceberg", nombre


def test_el_job_de_silver_carga_las_librerias_de_iceberg(dev):
    """Sin --datalake-formats, los JAR de Iceberg no estan en el classpath y el
    primer CREATE TABLE ... USING iceberg falla."""
    _, glue_stack = dev
    jobs = glue_stack.find_resources("AWS::Glue::Job")
    silver = next(j for j in jobs.values() if j["Properties"]["Name"].endswith("silver-transform"))
    assert silver["Properties"]["DefaultArguments"]["--datalake-formats"] == "iceberg"


def test_todos_los_jobs_reciben_el_paquete_comun(dev):
    """Glue no ve el codigo del repositorio. Sin --extra-py-files, cualquier
    `import common.algo` revienta nada mas arrancar."""
    _, glue_stack = dev
    for job in glue_stack.find_resources("AWS::Glue::Job").values():
        assert "--extra-py-files" in job["Properties"]["DefaultArguments"], job["Properties"][
            "Name"
        ]


def test_el_job_usa_glue_5(dev):
    _, glue_stack = dev
    glue_stack.has_resource_properties("AWS::Glue::Job", {"GlueVersion": "5.0"})


def test_el_rol_de_glue_puede_crear_enis(dev):
    """AWSGlueServiceRole trae los permisos de EC2 para las ENIs. Sin ellos el
    job con Connection se cuelga sin dar un error util."""
    _, glue_stack = dev
    roles = glue_stack.find_resources("AWS::IAM::Role")
    politicas = json.dumps([r["Properties"].get("ManagedPolicyArns", []) for r in roles.values()])
    assert "AWSGlueServiceRole" in politicas


def test_el_job_no_lleva_credenciales_en_los_argumentos(dev):
    """Los argumentos de un job son visibles en la consola de Glue. Ahi solo
    puede ir el NOMBRE del secreto, nunca su contenido."""
    _, glue_stack = dev
    jobs = glue_stack.find_resources("AWS::Glue::Job")
    args = next(iter(jobs.values()))["Properties"]["DefaultArguments"]
    # El nombre literal del secreto, no una expresion que trocee el ARN.
    assert args["--SECRET_ID"] == "practica/dev/postgres"
    texto = json.dumps(args)
    for sospechoso in ("password", "PASSWORD", "practica_admin"):
        assert sospechoso not in texto
