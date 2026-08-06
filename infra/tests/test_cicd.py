"""Tests del rol que GitHub Actions asume en AWS.

Aqui no se comprueba "que el CDK genere un rol". Se fijan los errores que
convierten este stack en una puerta abierta y que ninguna revision visual
detecta, porque la plantilla se ve igual de bien con ellos que sin ellos:

  * un trust policy sin condicion sobre `sub`: CUALQUIER repositorio de GitHub
    puede asumir el rol, y el ARN esta escrito en el workflow de un repo publico,
  * un `sub` con comodin (`repo:owner/repo:*`), que incluye los pull_request de
    forks,
  * un rol de produccion que confia en la rama en vez de en el Environment, con
    lo que el revisor obligatorio deja de ser obligatorio,
  * permisos directos sobre los recursos en vez de asumir los roles del
    bootstrap, que convierte una fuga del token en control total de la cuenta.

Ninguno de estos fallos da error al desplegar. Todos funcionan perfectamente.
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

from stacks.cicd_stack import BOOTSTRAP_ROLES, OIDC_HOST, QUALIFIER, CicdStack  # noqa: E402

ENV = cdk.Environment(account="123456789012", region="eu-west-1")
REPO = "vicentesanchezrincon/practica"


def build(**context) -> Template:
    app = cdk.App(context=context)
    stack = CicdStack(app, "T-Cicd", repo=REPO, env=ENV)
    return Template.from_stack(stack)


@pytest.fixture(scope="module")
def plantilla() -> Template:
    return build()


def roles_del_proyecto(plantilla: Template) -> dict[str, dict]:
    """Solo nuestros roles, por nombre.

    Se filtra a proposito: `OpenIdConnectProvider` es un custom resource y trae
    su propia Lambda con su propio rol de ejecucion. Contar todos los
    AWS::IAM::Role mezclaria ese detalle de implementacion del CDK con lo que
    aqui se quiere fijar.
    """
    return {
        recurso["Properties"]["RoleName"]: recurso
        for recurso in plantilla.find_resources("AWS::IAM::Role").values()
        if str(recurso["Properties"].get("RoleName", "")).startswith("practica-github-")
    }


def politicas_del_proyecto(plantilla: Template) -> dict[str, dict]:
    return {
        logico: recurso
        for logico, recurso in plantilla.find_resources("AWS::IAM::Policy").items()
        if logico.startswith("GitHubActions")
    }


def trust_policy(plantilla: Template, nombre_rol: str) -> dict:
    """El documento de confianza del rol indicado, ya como diccionario."""
    roles = roles_del_proyecto(plantilla)
    if nombre_rol not in roles:
        raise AssertionError(f"No existe el rol {nombre_rol}: hay {list(roles)}")
    return roles[nombre_rol]["Properties"]["AssumeRolePolicyDocument"]


def condiciones(plantilla: Template, nombre_rol: str) -> dict:
    return trust_policy(plantilla, nombre_rol)["Statement"][0]["Condition"]


def subs(plantilla: Template, nombre_rol: str) -> list[str]:
    valor = condiciones(plantilla, nombre_rol)["StringLike"][f"{OIDC_HOST}:sub"]
    return valor if isinstance(valor, list) else [valor]


# ------------------------------------------------------------- estructura ---


def test_se_crean_los_dos_roles(plantilla):
    assert sorted(roles_del_proyecto(plantilla)) == [
        "practica-github-dev",
        "practica-github-prod",
    ]


def test_el_proveedor_apunta_a_github(plantilla):
    plantilla.has_resource_properties(
        "Custom::AWSCDKOpenIdConnectProvider", {"Url": f"https://{OIDC_HOST}"}
    )


# ---------------------------------------------------- la condicion del sub ---


def test_el_trust_policy_exige_el_claim_sub(plantilla):
    """Sin esta condicion, el emisor y el `aud` son identicos para todos los
    repositorios de GitHub: cualquiera podria asumir el rol."""
    for rol in ("practica-github-dev", "practica-github-prod"):
        assert f"{OIDC_HOST}:sub" in condiciones(plantilla, rol).get("StringLike", {})


def test_el_sub_esta_anclado_a_este_repositorio(plantilla):
    for rol in ("practica-github-dev", "practica-github-prod"):
        for sub in subs(plantilla, rol):
            assert sub.startswith(f"repo:{REPO}:")


def test_ningun_sub_lleva_comodin(plantilla):
    """`repo:owner/repo:*` incluye los pull_request, y el token de un PR se emite
    contra el repositorio base: un PR desde un fork desplegaria con este rol."""
    for rol in ("practica-github-dev", "practica-github-prod"):
        for sub in subs(plantilla, rol):
            assert "*" not in sub, f"{rol} acepta el comodin {sub}"


def test_el_aud_es_sts_amazonaws_com(plantilla):
    """Sin comprobar el `aud` aceptarias tokens emitidos para otro publico."""
    for rol in ("practica-github-dev", "practica-github-prod"):
        assert condiciones(plantilla, rol)["StringEquals"] == {
            f"{OIDC_HOST}:aud": "sts.amazonaws.com"
        }


# -------------------------------------------------- separacion dev / prod ---


def test_prod_solo_confia_en_el_environment_no_en_la_rama(plantilla):
    """El claim `environment:prod` solo aparece si el job declara
    `environment: prod`, que es lo unico que dispara el revisor obligatorio.

    Aceptando ademas `refs/heads/main`, un job sin `environment:` desplegaria
    produccion saltandose la aprobacion, y la puerta seguiria pintada en la
    interfaz sin cerrar nada.
    """
    for sub in subs(plantilla, "practica-github-prod"):
        assert "refs/heads" not in sub
        assert sub.endswith(":environment:prod")


def test_dev_no_puede_desplegar_prod(plantilla):
    for sub in subs(plantilla, "practica-github-dev"):
        assert "environment:prod" not in sub


# ------------------------------------------------------------- permisos ----


def test_el_rol_no_tiene_permisos_directos_sobre_los_recursos(plantilla):
    """Solo sabe asumir los roles del bootstrap y leer su version.

    La alternativa -AdministratorAccess- desplegaria igual de bien y convertiria
    un push malicioso a develop en control total de la cuenta.
    """
    permitidas = {"sts:AssumeRole", "ssm:GetParameter"}
    for politica in politicas_del_proyecto(plantilla).values():
        for sentencia in politica["Properties"]["PolicyDocument"]["Statement"]:
            acciones = sentencia["Action"]
            for accion in acciones if isinstance(acciones, list) else [acciones]:
                assert accion in permitidas, f"permiso de mas: {accion}"


def test_los_roles_no_llevan_politicas_gestionadas(plantilla):
    for nombre, rol in roles_del_proyecto(plantilla).items():
        assert not rol["Properties"].get("ManagedPolicyArns"), nombre


def test_se_asumen_los_cuatro_roles_del_bootstrap(plantilla):
    """Faltando `lookup` o `file-publishing`, el despliegue falla a mitad y el
    mensaje no menciona cual de los cuatro es."""
    texto = json.dumps(politicas_del_proyecto(plantilla))
    for rol in BOOTSTRAP_ROLES:
        assert f"cdk-{QUALIFIER}-{rol}-role-" in texto


def test_se_puede_leer_la_version_del_bootstrap(plantilla):
    """El CLI del CDK lee este parametro ANTES de asumir ningun rol. Sin el
    permiso, el deploy muere con un AccessDenied sobre SSM que no dice por que."""
    texto = json.dumps(politicas_del_proyecto(plantilla))
    assert f"cdk-bootstrap/{QUALIFIER}/version" in texto


# ------------------------------------------------ el proveedor ya existente ---


def test_con_oidc_existente_no_se_crea_el_proveedor():
    """Una cuenta admite un solo proveedor por URL. Crear otro da
    EntityAlreadyExists; y destruir este stack se llevaria el que usan los demas
    repositorios de la cuenta."""
    importado = build(oidc_existente="true")
    importado.resource_count_is("Custom::AWSCDKOpenIdConnectProvider", 0)
    # Y sin custom resource tampoco queda su Lambda ni el rol de esta.
    importado.resource_count_is("AWS::Lambda::Function", 0)
    assert len(roles_del_proyecto(importado)) == 2


def test_el_flag_desactivado_como_cadena_no_cuenta_como_activo():
    """`-c oidc_existente=false` llega como la CADENA "false", que es truthy en
    Python. Si se comparara con un truthy pelado, apagar el flag lo encenderia."""
    build(oidc_existente="false").resource_count_is("Custom::AWSCDKOpenIdConnectProvider", 1)
