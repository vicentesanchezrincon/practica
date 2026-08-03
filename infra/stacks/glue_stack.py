"""Rol de ejecucion de Glue y jobs.

De momento contiene un unico job, `seed-rds`, que es una utilidad, no parte del
pipeline: carga el esquema y los datos de prueba en el RDS. Existe porque el
RDS vive en subredes aisladas y no se puede sembrar con un `psql` desde fuera.

Los jobs del pipeline (bronze, silver, gold) se anaden a este mismo stack en
las fases siguientes.
"""

from __future__ import annotations

from pathlib import Path

from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3deploy
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct

ROOT = Path(__file__).resolve().parents[2]
JOBS_DIR = ROOT / "src" / "jobs"
DATA_GENERATOR_DIR = ROOT / "data_generator"
SCRIPTS_PREFIX = "scripts"
SEED_PREFIX = "_seed"

GLUE_VERSION = "5.0"


class GlueStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        environment: str,
        bucket: s3.IBucket,
        secret: secretsmanager.ISecret,
        secret_name: str,
        connection_name: str,
        glue_security_group: ec2.ISecurityGroup,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment_name = environment
        self.bucket = bucket

        self.role = self._create_role(bucket, secret)
        self._deploy_scripts(bucket)

        self.seed_job = self._create_seed_job(bucket, secret_name, connection_name)

        CfnOutput(self, "GlueRoleArn", value=self.role.role_arn)
        CfnOutput(self, "SeedJobName", value=self.seed_job.ref)
        CfnOutput(
            self,
            "LanzarSiembra",
            value=f"aws glue start-job-run --job-name practica-{environment}-seed-rds",
            description="Comando para sembrar el RDS",
        )

    # ------------------------------------------------------------------ rol ---

    def _create_role(self, bucket: s3.IBucket, secret: secretsmanager.ISecret) -> iam.Role:
        role = iam.Role(
            self,
            "GlueJobRole",
            role_name=f"practica-{self.environment_name}-glue-job",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Rol de ejecucion de los jobs de Glue",
            managed_policies=[
                # Incluye los permisos de EC2 para crear las ENIs dentro de la
                # VPC. Sin ellos, un job con Connection se cuelga sin explicar
                # por que: es el error mas comun al meter Glue en una VPC.
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
            ],
        )

        bucket.grant_read_write(role)
        secret.grant_read(role)
        return role

    # -------------------------------------------------------------- scripts ---

    def _deploy_scripts(self, bucket: s3.IBucket) -> None:
        """Sube los scripts de los jobs a S3 en cada despliegue.

        Glue no ejecuta codigo local: lee el script desde S3. Con esto, un
        `cdk deploy` publica los cambios del codigo igual que los de la
        infraestructura, sin pasos manuales que olvidar.
        """
        s3deploy.BucketDeployment(
            self,
            "JobScripts",
            sources=[
                s3deploy.Source.asset(str(JOBS_DIR)),
                # El DDL tambien: asi el job de siembra lee el mismo schema.sql
                # que usas en local, en vez de una copia que acabaria divergiendo.
                s3deploy.Source.asset(str(DATA_GENERATOR_DIR), exclude=["*", "!schema.sql"]),
            ],
            destination_bucket=bucket,
            destination_key_prefix=SCRIPTS_PREFIX,
            # prune=False para no borrar otros objetos que haya bajo scripts/
            prune=False,
            retain_on_delete=False,
        )

    # ------------------------------------------------------------------ job ---

    def _create_seed_job(
        self,
        bucket: s3.IBucket,
        secret_name: str,
        connection_name: str,
    ) -> glue.CfnJob:
        return glue.CfnJob(
            self,
            "SeedRdsJob",
            name=f"practica-{self.environment_name}-seed-rds",
            description="Carga el esquema y los datos de prueba en el RDS desde S3",
            role=self.role.role_arn,
            glue_version=GLUE_VERSION,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{bucket.bucket_name}/{SCRIPTS_PREFIX}/seed_rds.py",
            ),
            # Esto es lo que mete el job dentro de la VPC.
            connections=glue.CfnJob.ConnectionsListProperty(connections=[connection_name]),
            worker_type="G.1X",
            number_of_workers=2,
            timeout=30,
            max_retries=0,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
            default_arguments={
                "--job-language": "python",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--TempDir": f"s3://{bucket.bucket_name}/temp/",
                # Parametros propios del script
                # El nombre literal, no secret.secret_name: ese resuelve a una
                # expresion que trocea el ARN por guiones.
                "--SECRET_ID": secret_name,
                "--SEED_PREFIX": f"s3://{bucket.bucket_name}/{SEED_PREFIX}",
                "--DB_NAME": "ecommerce",
            },
        )
