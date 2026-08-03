"""Red: VPC aislada donde correran Glue y RDS.

Decision de diseno principal: **no hay NAT Gateway**. Las subredes son
PRIVATE_ISOLATED puras, sin salida a internet. Todo lo que Glue necesita de
AWS entra por VPC endpoints.

Sobre el coste, que es donde la gente se lleva el susto:

  * El endpoint **Gateway** de S3 es gratis. Siempre se crea.
  * Los endpoints de **interfaz** cuestan ~0,01 USD/hora **por AZ** cada uno
    (~7-9 USD/mes). Con 4 endpoints en 2 AZ te vas a ~58 USD/mes, o sea MAS
    caro que un NAT Gateway (~33 USD/mes). Por eso aqui se despliegan en una
    sola AZ: el trafico entre AZ de PrivateLink es gratis desde abril de 2022,
    asi que funciona igual y cuesta la mitad.
  * Aun asi, si dejas esto desplegado un mes entero son ~29 USD. La forma
    correcta de usar este laboratorio es `make destroy-dev` al terminar cada
    sesion: unas horas sueltas cuestan centimos.

Si quieres desplegar solo la red para trastear sin pagar nada, apaga los
endpoints de interfaz:

    cdk deploy Practica-Dev-Network -c environment=dev -c interface_endpoints=false
"""

from __future__ import annotations

from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_ec2 as ec2
from constructs import Construct


class NetworkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, environment: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment_name = environment

        # 2 AZ no es por alta disponibilidad: es que un DB Subnet Group de RDS
        # exige subredes en dos AZ como minimo, aunque la instancia sea unica.
        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            vpc_name=f"practica-{environment}",
            ip_addresses=ec2.IpAddresses.cidr("10.20.0.0/16"),
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                )
            ],
            enable_dns_hostnames=True,
            enable_dns_support=True,
        )

        self._create_security_groups()
        self._create_endpoints()
        self._outputs()

    # ------------------------------------------------------------------ SGs ---

    def _create_security_groups(self) -> None:
        self.glue_sg = ec2.SecurityGroup(
            self,
            "GlueSecurityGroup",
            vpc=self.vpc,
            description="ENIs que AWS Glue crea dentro de la VPC",
            security_group_name=f"practica-{self.environment_name}-glue",
            allow_all_outbound=True,
        )

        # Requisito de AWS Glue, no un descuido: los nodos del cluster de Spark
        # se hablan entre si por puertos arbitrarios, y Glue exige una regla que
        # permita TODO el trafico desde el propio security group. Sin esto el
        # job se queda colgado y acaba fallando por timeout sin explicar por que.
        self.glue_sg.add_ingress_rule(
            peer=self.glue_sg,
            connection=ec2.Port.all_traffic(),
            description="Requisito de Glue: los nodos del cluster se comunican entre si",
        )

        self.rds_sg = ec2.SecurityGroup(
            self,
            "RdsSecurityGroup",
            vpc=self.vpc,
            description="Postgres de origen. Solo accesible desde Glue.",
            security_group_name=f"practica-{self.environment_name}-rds",
            allow_all_outbound=False,
        )
        self.rds_sg.add_ingress_rule(
            peer=self.glue_sg,
            connection=ec2.Port.tcp(5432),
            description="Postgres desde los jobs de Glue",
        )

        self.endpoint_sg = ec2.SecurityGroup(
            self,
            "EndpointSecurityGroup",
            vpc=self.vpc,
            description="Interfaces de los VPC endpoints",
            security_group_name=f"practica-{self.environment_name}-endpoints",
            allow_all_outbound=False,
        )
        self.endpoint_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4(self.vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(443),
            description="HTTPS desde dentro de la VPC",
        )

    # ------------------------------------------------------------ endpoints ---

    def _create_endpoints(self) -> None:
        # Gratis, y sin el los jobs no pueden ni leer su propio script.
        self.vpc.add_gateway_endpoint("S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3)

        if self.node.try_get_context("interface_endpoints") is False:
            return

        # Una sola subred: ver la nota de coste en el docstring del modulo.
        single_az = ec2.SubnetSelection(subnets=[self.vpc.isolated_subnets[0]])

        needed = {
            # Data Catalog y API de Glue (bookmarks, estado del job)
            "Glue": ec2.InterfaceVpcEndpointAwsService.GLUE,
            # Sin esto no veras ni un log del job. Depurar a ciegas es inviable.
            "CloudWatchLogs": ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
            # Credenciales del Postgres (Fase 3)
            "SecretsManager": ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            # Asuncion de roles desde dentro de la VPC
            "Sts": ec2.InterfaceVpcEndpointAwsService.STS,
        }

        self.interface_endpoints = {
            name: self.vpc.add_interface_endpoint(
                f"{name}Endpoint",
                service=service,
                subnets=single_az,
                security_groups=[self.endpoint_sg],
                private_dns_enabled=True,
            )
            for name, service in needed.items()
        }

    # -------------------------------------------------------------- outputs ---

    def _outputs(self) -> None:
        CfnOutput(self, "VpcId", value=self.vpc.vpc_id)
        CfnOutput(
            self,
            "IsolatedSubnetIds",
            value=",".join(s.subnet_id for s in self.vpc.isolated_subnets),
        )
        CfnOutput(self, "GlueSecurityGroupId", value=self.glue_sg.security_group_id)
        CfnOutput(self, "RdsSecurityGroupId", value=self.rds_sg.security_group_id)
