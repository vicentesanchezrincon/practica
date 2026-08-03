"""Almacenamiento: el bucket del data lake y las bases del Glue Data Catalog.

Un solo bucket con prefijos por capa (bronze/silver/gold) en vez de tres
buckets. Es lo habitual: las politicas de acceso se hacen igual de bien por
prefijo, y evitas multiplicar configuracion.

Las tres bases del catalogo se declaran aqui, no con un Crawler. Un crawler
cuesta dinero cada vez que corre, tarda, y adivina el esquema, lo que provoca
sorpresas cuando cambia el origen. Declarar el esquema es determinista.
"""

from __future__ import annotations

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_glue as glue
from aws_cdk import aws_s3 as s3
from constructs import Construct

from common.config import catalog_database

LAYERS = ("bronze", "silver", "gold")


class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, environment: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment_name = environment
        is_prod = environment == "prod"

        # Los nombres de bucket son globales en todo AWS, de ahi la cuenta y la
        # region en el nombre: asi el mismo codigo se despliega en otra cuenta
        # sin colisionar.
        self.bucket = s3.Bucket(
            self,
            "DataLake",
            bucket_name=f"practica-datalake-{environment}-{self.account}-{self.region}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            # En prod, borrar el stack NO debe llevarse los datos por delante.
            # En dev si: es un laboratorio y quieres poder empezar de cero.
            removal_policy=RemovalPolicy.RETAIN if is_prod else RemovalPolicy.DESTROY,
            auto_delete_objects=not is_prod,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="limpiar-temporales-de-glue",
                    prefix="temp/",
                    expiration=Duration.days(7),
                ),
                s3.LifecycleRule(
                    id="limpiar-resultados-de-athena",
                    prefix="athena-results/",
                    expiration=Duration.days(30),
                ),
                s3.LifecycleRule(
                    id="limpiar-versiones-antiguas",
                    noncurrent_version_expiration=Duration.days(30),
                    # Iceberg reescribe metadatos constantemente. Sin esto, cada
                    # version antigua se queda pagando almacenamiento para siempre.
                    abort_incomplete_multipart_upload_after=Duration.days(7),
                ),
            ],
        )

        # `catalog_database` viene de src/common/config.py, el mismo modulo que
        # importan los jobs. Si alguien cambia la convencion de nombres, cambia
        # en los dos sitios a la vez.
        self.databases = {
            layer: glue.CfnDatabase(
                self,
                f"{layer.capitalize()}Database",
                catalog_id=self.account,
                database_input=glue.CfnDatabase.DatabaseInputProperty(
                    name=catalog_database(layer, environment),
                    description=f"Capa {layer} del data lake ({environment})",
                    location_uri=f"s3://{self.bucket.bucket_name}/{layer}/",
                ),
            )
            for layer in LAYERS
        }

        self._outputs()

    def _outputs(self) -> None:
        CfnOutput(self, "BucketName", value=self.bucket.bucket_name)
        CfnOutput(self, "BucketArn", value=self.bucket.bucket_arn)
        for layer in LAYERS:
            # Ojo con el id: no puede coincidir con el del CfnDatabase de arriba,
            # los constructs comparten espacio de nombres dentro del stack.
            CfnOutput(
                self,
                f"{layer.capitalize()}DatabaseName",
                value=catalog_database(layer, self.environment_name),
            )
