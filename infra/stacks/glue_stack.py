"""Rol de ejecucion de Glue y jobs.

Jobs actuales:

  * `seed-rds`  utilidad, no pipeline: carga el esquema y los datos de prueba
                en el RDS. Existe porque el RDS vive en subredes aisladas y no
                se puede sembrar con un `psql` desde fuera.
  * `bronze-ingest`  extraccion incremental del RDS a la capa Bronze.

Silver y Gold se anaden a este mismo stack en las fases siguientes.

Todos los jobs reciben `--extra-py-files` con el paquete src/common empaquetado
en un zip. Glue no ve el codigo del repositorio: solo lo que subimos a S3.
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
BUILD_DIR = ROOT / "build"
SCRIPTS_PREFIX = "scripts"
SEED_PREFIX = "_seed"

GLUE_VERSION = "5.0"

# Paquete con el codigo compartido (src/common). Glue no ejecuta codigo local,
# asi que hay que subirselo y pasarselo con --extra-py-files. Lo construye
# `make build-common`, del que dependen synth y deploy.
COMMON_ZIP = "common.zip"


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
        self.bronze_job = self._create_bronze_job(bucket, secret_name, connection_name)

        CfnOutput(self, "GlueRoleArn", value=self.role.role_arn)
        CfnOutput(self, "SeedJobName", value=self.seed_job.ref)
        CfnOutput(self, "BronzeJobName", value=self.bronze_job.ref)
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

        # Watermarks de la extraccion incremental. Se acota al prefijo del
        # entorno: un job de dev no debe poder mover el watermark de prod.
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["ssm:GetParameter", "ssm:GetParameters", "ssm:PutParameter"],
                resources=[
                    Stack.of(self).format_arn(
                        service="ssm",
                        resource="parameter",
                        resource_name=f"practica/{self.environment_name}/watermark/*",
                    )
                ],
            )
        )
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
                # El paquete src/common empaquetado. Lo genera `make build-common`.
                s3deploy.Source.asset(str(BUILD_DIR)),
            ],
            destination_bucket=bucket,
            destination_key_prefix=SCRIPTS_PREFIX,
            # prune=False para no borrar otros objetos que haya bajo scripts/
            prune=False,
            retain_on_delete=False,
        )

    # ----------------------------------------------------------------- jobs ---

    def _base_arguments(self, bucket: s3.IBucket) -> dict[str, str]:
        """Argumentos comunes a todos los jobs."""
        return {
            "--job-language": "python",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--TempDir": f"s3://{bucket.bucket_name}/temp/",
            # Sin esto, `import common.config` falla en el job: Glue no ve el
            # codigo del repositorio, solo lo que le subimos a S3.
            "--extra-py-files": f"s3://{bucket.bucket_name}/{SCRIPTS_PREFIX}/{COMMON_ZIP}",
        }

    def _create_bronze_job(
        self,
        bucket: s3.IBucket,
        secret_name: str,
        connection_name: str,
    ) -> glue.CfnJob:
        return glue.CfnJob(
            self,
            "BronzeIngestJob",
            name=f"practica-{self.environment_name}-bronze-ingest",
            description="Extraccion incremental del RDS a la capa Bronze",
            role=self.role.role_arn,
            glue_version=GLUE_VERSION,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{bucket.bucket_name}/{SCRIPTS_PREFIX}/bronze_ingest.py",
            ),
            connections=glue.CfnJob.ConnectionsListProperty(connections=[connection_name]),
            worker_type="G.1X",
            number_of_workers=2,
            timeout=60,
            # 1 reintento: los fallos transitorios de red en una VPC son reales,
            # y el job es seguro de repetir (el watermark solo avanza al final).
            max_retries=1,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
            default_arguments={
                **self._base_arguments(bucket),
                "--ENVIRONMENT": self.environment_name,
                "--BUCKET": bucket.bucket_name,
                "--SECRET_ID": secret_name,
                "--DB_NAME": "ecommerce",
            },
        )

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
                **self._base_arguments(bucket),
                # El nombre literal, no secret.secret_name: ese resuelve a una
                # expresion que trocea el ARN por guiones.
                "--SECRET_ID": secret_name,
                "--SEED_PREFIX": f"s3://{bucket.bucket_name}/{SEED_PREFIX}",
                "--DB_NAME": "ecommerce",
            },
        )
