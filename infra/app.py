#!/usr/bin/env python3
"""Punto de entrada de la infraestructura.

Un unico app para dos entornos. El entorno se elige por contexto:

    cdk synth  -c environment=dev
    cdk deploy --all -c environment=prod

Los stacks quedan como Practica-Dev-Network, Practica-Prod-Storage, etc., asi
que dev y prod pueden convivir en la misma cuenta sin pisarse.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import aws_cdk as cdk

# El codigo de los jobs y el de la infraestructura tienen que estar de acuerdo
# en como se llaman las bases del catalogo y las rutas del lake. En vez de
# duplicar esas reglas aqui, importamos las mismas funciones que usan los jobs.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from stacks.cicd_stack import CicdStack  # noqa: E402
from stacks.database_stack import DatabaseStack  # noqa: E402
from stacks.glue_stack import GlueStack  # noqa: E402
from stacks.network_stack import NetworkStack  # noqa: E402
from stacks.orchestration_stack import OrchestrationStack  # noqa: E402
from stacks.storage_stack import StorageStack  # noqa: E402

VALID_ENVIRONMENTS = ("dev", "prod")

app = cdk.App()

environment = app.node.try_get_context("environment") or "dev"
if environment not in VALID_ENVIRONMENTS:
    raise SystemExit(
        f"environment='{environment}' no es valido. Usa uno de: {', '.join(VALID_ENVIRONMENTS)}\n"
        f"Ejemplo: cdk synth -c environment=dev"
    )

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

prefix = f"Practica-{environment.capitalize()}"

network = NetworkStack(
    app,
    f"{prefix}-Network",
    environment=environment,
    env=env,
    description="VPC aislada, security groups y VPC endpoints para Glue y RDS",
)

storage = StorageStack(
    app,
    f"{prefix}-Storage",
    environment=environment,
    env=env,
    description="Bucket del data lake (bronze/silver/gold) y bases del Glue Data Catalog",
)

database = DatabaseStack(
    app,
    f"{prefix}-Database",
    environment=environment,
    env=env,
    vpc=network.vpc,
    rds_security_group=network.rds_sg,
    glue_security_group=network.glue_sg,
    description="Postgres de origen en RDS y la Glue Connection que lo alcanza",
)

glue_stack = GlueStack(
    app,
    f"{prefix}-Glue",
    environment=environment,
    env=env,
    bucket=storage.bucket,
    secret=database.secret,
    secret_name=database.secret_name,
    connection_name=database.connection_name,
    glue_security_group=network.glue_sg,
    description="Rol de ejecucion y jobs de Glue",
)

orchestration = OrchestrationStack(
    app,
    f"{prefix}-Orchestration",
    environment=environment,
    env=env,
    bucket=storage.bucket,
    bronze_job=glue_stack.bronze_job,
    files_job=glue_stack.files_job,
    silver_job=glue_stack.silver_job,
    gold_job=glue_stack.gold_job,
    # Opcional: cdk deploy ... -c alert_email=tu@correo.com
    alert_email=app.node.try_get_context("alert_email"),
    description="Maquina de estados del pipeline, alertas y ejecucion programada",
)
orchestration.add_stack_dependency(glue_stack)

# CloudFormation deduce casi todas las dependencias de las referencias cruzadas,
# pero el job declara la Connection por nombre (un string), no por referencia.
# Sin esta linea, Glue podria desplegarse antes de que la Connection exista.
glue_stack.add_stack_dependency(database)

# El stack de CI/CD solo se construye si lo pides explicitamente.
#
# No es cosmetica: `cdk deploy --all` y `cdk destroy --all` actuan sobre los
# stacks que este fichero CONSTRUYE. Si no se construye, no existe para ellos, y
# el `make destroy-dev` del final de cada sesion no puede llevarselo por delante.
# Confiar en un `--exclusively` o en llamarlo "no-tocar" dependeria de que te
# acuerdes justo el dia que tengas prisa.
#
# El `str(...).lower() == "true"` tampoco es manias: `-c cicd=false` llega como
# la CADENA "false", que en Python es verdadera. Ese fallo lo comete todo el
# mundo una vez.
if str(app.node.try_get_context("cicd")).lower() == "true":
    CicdStack(
        app,
        "Practica-Cicd",
        env=env,
        repo=app.node.try_get_context("github_repo") or "vicentesanchezrincon/practica",
        # Segundo cinturon: si algun dia el stack acabara dentro de un --all por
        # error, CloudFormation se niega a borrarlo.
        termination_protection=True,
        description="Proveedor OIDC de GitHub y roles que asume GitHub Actions",
    )

# Etiquetas en todo lo que se cree: sin esto es imposible saber despues que
# recurso de la factura pertenece a que proyecto.
cdk.Tags.of(app).add("Project", "practica")
cdk.Tags.of(app).add("Environment", environment)
cdk.Tags.of(app).add("ManagedBy", "cdk")

app.synth()
