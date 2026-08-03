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

from stacks.network_stack import NetworkStack  # noqa: E402
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

# Etiquetas en todo lo que se cree: sin esto es imposible saber despues que
# recurso de la factura pertenece a que proyecto.
cdk.Tags.of(app).add("Project", "practica")
cdk.Tags.of(app).add("Environment", environment)
cdk.Tags.of(app).add("ManagedBy", "cdk")

app.synth()
