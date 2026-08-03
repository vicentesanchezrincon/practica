"""Tests de la infraestructura.

Se ejecutan contra la plantilla de CloudFormation que genera el CDK, sin tocar
AWS ni gastar un centimo. Su valor no es comprobar que el CDK funciona, sino
fijar las decisiones que si se rompen cuestan dinero o rompen los jobs:

  * que no aparezca un NAT Gateway por descuido,
  * que los endpoints de interfaz sigan en una sola AZ,
  * que el security group de Glue conserve la regla que se permite a si mismo,
  * que el bucket de prod no se pueda borrar con un `cdk destroy`.

Corren en el venv del host (necesitan aws-cdk-lib), no en el contenedor:

    make test-infra
"""

from __future__ import annotations

import sys
from pathlib import Path

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent / "src"))

from stacks.network_stack import NetworkStack  # noqa: E402
from stacks.storage_stack import StorageStack  # noqa: E402

ENV = cdk.Environment(account="123456789012", region="eu-west-1")


def build(stack_cls, environment: str = "dev", **context):
    app = cdk.App(context=context)
    stack = stack_cls(app, f"Test-{stack_cls.__name__}", environment=environment, env=ENV)
    return Template.from_stack(stack)


@pytest.fixture(scope="module")
def network() -> Template:
    return build(NetworkStack)


@pytest.fixture(scope="module")
def storage_dev() -> Template:
    return build(StorageStack, "dev")


@pytest.fixture(scope="module")
def storage_prod() -> Template:
    return build(StorageStack, "prod")


# ------------------------------------------------------------------- red ----


def test_no_hay_nat_gateway(network):
    """~33 USD/mes que aparecen solos si alguien pone nat_gateways=1."""
    network.resource_count_is("AWS::EC2::NatGateway", 0)
    network.resource_count_is("AWS::EC2::EIP", 0)


def test_no_hay_salida_a_internet(network):
    network.resource_count_is("AWS::EC2::InternetGateway", 0)


def test_las_subredes_son_aisladas(network):
    """Sin MapPublicIpOnLaunch: nada en esta VPC recibe IP publica."""
    for subnet in network.find_resources("AWS::EC2::Subnet").values():
        assert subnet["Properties"].get("MapPublicIpOnLaunch") is not True


def test_el_endpoint_de_s3_es_gateway(network):
    """El Gateway es gratis; si alguien lo convierte en Interface, empieza a costar."""
    network.has_resource_properties(
        "AWS::EC2::VPCEndpoint",
        {"VpcEndpointType": "Gateway", "ServiceName": Match.any_value()},
    )


def test_los_endpoints_de_interfaz_estan_en_una_sola_az(network):
    """Se paga por endpoint Y por AZ. Dos AZ duplican la factura sin aportar
    nada en un laboratorio: el trafico entre AZ de PrivateLink es gratis."""
    interfaces = [
        r
        for r in network.find_resources("AWS::EC2::VPCEndpoint").values()
        if r["Properties"].get("VpcEndpointType") == "Interface"
    ]
    assert interfaces, "no se creo ningun endpoint de interfaz"
    for endpoint in interfaces:
        assert len(endpoint["Properties"]["SubnetIds"]) == 1


def test_se_pueden_apagar_los_endpoints_de_interfaz(network):
    """El flag existe para poder desplegar la red sin coste horario."""
    sin_endpoints = build(NetworkStack, interface_endpoints=False)
    interfaces = [
        r
        for r in sin_endpoints.find_resources("AWS::EC2::VPCEndpoint").values()
        if r["Properties"].get("VpcEndpointType") == "Interface"
    ]
    assert interfaces == []
    # El de S3 es gratis, ese se queda siempre.
    sin_endpoints.resource_count_is("AWS::EC2::VPCEndpoint", 1)


def test_el_sg_de_glue_se_permite_a_si_mismo(network):
    """Requisito de Glue. Sin esta regla el job se cuelga y falla por timeout
    sin dar un mensaje util."""
    network.has_resource_properties(
        "AWS::EC2::SecurityGroupIngress",
        {
            "IpProtocol": "-1",
            "GroupId": Match.any_value(),
            "SourceSecurityGroupId": Match.any_value(),
        },
    )


def test_rds_solo_acepta_postgres_desde_glue(network):
    network.has_resource_properties(
        "AWS::EC2::SecurityGroupIngress",
        {"IpProtocol": "tcp", "FromPort": 5432, "ToPort": 5432},
    )


def test_ningun_security_group_abre_al_mundo(network):
    for sg in network.find_resources("AWS::EC2::SecurityGroup").values():
        for rule in sg["Properties"].get("SecurityGroupIngress", []):
            assert rule.get("CidrIp") != "0.0.0.0/0", sg["Properties"].get("GroupName")


# ------------------------------------------------------------ almacenamiento ---


def test_el_bucket_bloquea_el_acceso_publico(storage_dev):
    storage_dev.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            }
        },
    )


def test_el_bucket_esta_cifrado_y_versionado(storage_dev):
    storage_dev.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "VersioningConfiguration": {"Status": "Enabled"},
            "BucketEncryption": Match.any_value(),
        },
    )


def test_el_bucket_de_dev_se_puede_destruir(storage_dev):
    """En un laboratorio quieres poder empezar de cero."""
    storage_dev.has_resource("AWS::S3::Bucket", {"DeletionPolicy": "Delete"})


def test_el_bucket_de_prod_sobrevive_a_un_destroy(storage_prod):
    """La red de seguridad que evita perder datos por un comando mal escrito."""
    storage_prod.has_resource("AWS::S3::Bucket", {"DeletionPolicy": "Retain"})
    storage_prod.resource_count_is("Custom::S3AutoDeleteObjects", 0)


def test_los_temporales_de_glue_caducan(storage_dev):
    """Sin esto, el directorio temp/ de Glue crece para siempre."""
    bucket = next(iter(storage_dev.find_resources("AWS::S3::Bucket").values()))
    reglas = {
        r["Prefix"]: r
        for r in bucket["Properties"]["LifecycleConfiguration"]["Rules"]
        if "Prefix" in r
    }
    assert reglas["temp/"]["ExpirationInDays"] == 7


def test_se_crean_las_tres_capas_del_catalogo(storage_dev):
    storage_dev.resource_count_is("AWS::Glue::Database", 3)
    nombres = {
        r["Properties"]["DatabaseInput"]["Name"]
        for r in storage_dev.find_resources("AWS::Glue::Database").values()
    }
    assert nombres == {"practica_dev_bronze", "practica_dev_silver", "practica_dev_gold"}


def test_dev_y_prod_no_colisionan(storage_dev, storage_prod):
    """Los dos entornos tienen que poder convivir en la misma cuenta."""

    def nombre(t):
        return next(iter(t.find_resources("AWS::S3::Bucket").values()))["Properties"]["BucketName"]

    assert nombre(storage_dev) != nombre(storage_prod)
