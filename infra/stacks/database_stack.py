"""El sistema origen: un Postgres gestionado en RDS.

Es el equivalente en AWS del contenedor de Postgres que usas en local. Vive en
las subredes aisladas de la Fase 2, asi que **no es accesible desde tu portatil**
ni desde internet. Solo lo alcanza lo que este dentro de la VPC, que en la
practica significa los jobs de Glue.

Ese aislamiento es deliberado y es como se hace en produccion, pero tiene una
consecuencia: para cargarle el esquema y los datos hace falta un job de Glue
(ver `glue_stack.py`), no un `psql` desde tu maquina.

La contrasena no aparece en ningun sitio del codigo: la genera AWS y la guarda
en Secrets Manager. Ni siquiera nosotros la vemos.
"""

from __future__ import annotations

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_glue as glue
from aws_cdk import aws_rds as rds
from constructs import Construct

DATABASE_NAME = "ecommerce"
POSTGRES_PORT = 5432


def secret_name_for(environment: str) -> str:
    """Nombre del secreto con las credenciales del RDS.

    Se fija explicitamente en vez de dejar que AWS genere uno aleatorio: asi
    puedes leerlo desde el Makefile o la CLI sin tener que buscar el ARN.
    """
    return f"practica/{environment}/postgres"


class DatabaseStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        environment: str,
        vpc: ec2.IVpc,
        rds_security_group: ec2.ISecurityGroup,
        glue_security_group: ec2.ISecurityGroup,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment_name = environment
        self.secret_name = secret_name_for(environment)
        is_prod = environment == "prod"

        self.instance = rds.DatabaseInstance(
            self,
            "Postgres",
            instance_identifier=f"practica-{environment}",
            engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_16_9),
            # db.t4g.micro entra en el free tier los primeros 12 meses.
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.BURSTABLE4_GRAVITON, ec2.InstanceSize.MICRO
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            security_groups=[rds_security_group],
            # AWS genera la contrasena y la guarda cifrada. El codigo nunca la toca.
            credentials=rds.Credentials.from_generated_secret(
                "practica_admin",
                secret_name=self.secret_name,
            ),
            database_name=DATABASE_NAME,
            port=POSTGRES_PORT,
            allocated_storage=20,
            max_allocated_storage=50,
            storage_encrypted=True,
            multi_az=False,
            publicly_accessible=False,
            # En dev, cero backups: aceleran el destroy y no hay nada que perder.
            backup_retention=Duration.days(7) if is_prod else Duration.days(0),
            delete_automated_backups=not is_prod,
            deletion_protection=is_prod,
            removal_policy=RemovalPolicy.RETAIN if is_prod else RemovalPolicy.DESTROY,
            # Performance Insights y los logs a CloudWatch cuestan dinero y en un
            # laboratorio no aportan nada.
            enable_performance_insights=False,
            auto_minor_version_upgrade=True,
        )

        self.secret = self.instance.secret
        assert self.secret is not None, "from_generated_secret siempre crea un secreto"

        self.connection = self._create_glue_connection(vpc, glue_security_group)
        self._outputs()

    # ----------------------------------------------------------- connection ---

    def _create_glue_connection(
        self, vpc: ec2.IVpc, glue_security_group: ec2.ISecurityGroup
    ) -> glue.CfnConnection:
        """La Glue Connection es lo que permite a un job entrar en la VPC.

        Sin ella, un job de Glue corre en la red de AWS y no ve el RDS. Con ella,
        Glue levanta ENIs en la subred que le indiquemos y el job pasa a estar
        dentro de nuestra red.

        Ojo con dos cosas que hacen perder tardes enteras:
          * la subred y la AZ tienen que ser coherentes entre si,
          * el security group debe permitirse a si mismo todo el trafico
            (lo configuramos en NetworkStack).
        """
        subnet = vpc.isolated_subnets[0]

        return glue.CfnConnection(
            self,
            "PostgresConnection",
            catalog_id=self.account,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name=f"practica-{self.environment_name}-postgres",
                description="Postgres de origen, dentro de la VPC",
                connection_type="JDBC",
                connection_properties={
                    "JDBC_CONNECTION_URL": (
                        f"jdbc:postgresql://{self.instance.db_instance_endpoint_address}:"
                        f"{self.instance.db_instance_endpoint_port}/{DATABASE_NAME}"
                    ),
                    # Glue lee usuario y contrasena del secreto en tiempo de
                    # ejecucion. Nunca viajan en la plantilla.
                    #
                    # Se pone el nombre literal y no self.secret.secret_name
                    # porque eso ultimo genera una expresion que trocea el ARN
                    # partiendolo por guiones: funciona de casualidad mientras
                    # el nombre no lleve ninguno.
                    "SECRET_ID": self.secret_name,
                    "JDBC_ENFORCE_SSL": "true",
                },
                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    availability_zone=subnet.availability_zone,
                    subnet_id=subnet.subnet_id,
                    security_group_id_list=[glue_security_group.security_group_id],
                ),
            ),
        )

    @property
    def connection_name(self) -> str:
        return f"practica-{self.environment_name}-postgres"

    # -------------------------------------------------------------- outputs ---

    def _outputs(self) -> None:
        CfnOutput(self, "DbEndpoint", value=self.instance.db_instance_endpoint_address)
        CfnOutput(self, "DbSecretName", value=self.secret_name)
        CfnOutput(self, "GlueConnectionName", value=self.connection_name)
        CfnOutput(
            self,
            "VerCredenciales",
            value=f"aws secretsmanager get-secret-value --secret-id {self.secret_name} --query SecretString --output text",
            description="Comando para leer usuario y contrasena si los necesitas",
        )
