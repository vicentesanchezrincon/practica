"""Identidad de GitHub Actions en AWS, sin claves estaticas.

La forma clasica de desplegar desde CI es crear un usuario IAM, sacarle una
clave de acceso y pegarla en los secrets del repositorio. Esa clave no caduca,
vive en un sistema de terceros y no hay forma de saber quien la ha copiado. Si
se filtra, sigue valiendo hasta que alguien se acuerde de rotarla.

Con OIDC no hay clave. GitHub firma un token de vida cortisima que describe
QUIEN esta ejecutando QUE (repositorio, rama, workflow, entorno), AWS lo valida
contra el certificado publico de GitHub y devuelve credenciales temporales. No
hay nada que rotar porque no hay nada guardado.

Este stack es distinto a todos los demas del proyecto en tres cosas:

  1. No pertenece a un entorno: hay UNO por cuenta, no uno de dev y otro de
     prod. Por eso se llama "Practica-Cicd" y no "Practica-Dev-Cicd".
  2. Es gratis (solo IAM) y hace falta para que el CI funcione, asi que NO debe
     desaparecer con el `make destroy-dev` del final de cada sesion.
  3. Es el huevo y la gallina: hay que desplegarlo a mano desde tu portatil,
     porque hasta que exista no hay rol que permita desplegar desde Actions.

Se despliega con:  make deploy-cicd
"""

from __future__ import annotations

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_iam as iam
from constructs import Construct

OIDC_HOST = "token.actions.githubusercontent.com"

AUDIENCE = "sts.amazonaws.com"
"""El `aud` que pide `aws-actions/configure-aws-credentials` por defecto."""

QUALIFIER = "hnb659fds"
"""Qualifier del bootstrap por defecto del CDK.

Si algun dia bootstrapeas la cuenta con `--qualifier`, hay que cambiarlo aqui o
los ARN de mas abajo apuntaran a roles que no existen. El sintoma es un
AccessDenied al desplegar que no menciona el bootstrap por ningun sitio.
"""

BOOTSTRAP_ROLES = ("deploy", "file-publishing", "lookup", "image-publishing")


class CicdStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, repo: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.repo = repo
        provider = self._provider()

        # Un rol por entorno, y con condiciones DISTINTAS a proposito.
        self.dev_role = self._deploy_role(
            "Dev",
            provider,
            # `workflow_dispatch` y `push` sobre develop producen este mismo
            # `sub`: el claim lleva la rama elegida, no el evento que disparo
            # el workflow.
            subs=[f"repo:{repo}:ref:refs/heads/develop"],
        )
        self.prod_role = self._deploy_role(
            "Prod",
            provider,
            # OJO: aqui NO va "ref:refs/heads/main", y no es un olvido.
            #
            # El claim `environment:prod` solo aparece si el job declara
            # `environment: prod`, y esa declaracion es lo unico que dispara la
            # regla de revisor obligatorio del GitHub Environment. Si tambien
            # aceptaramos el `ref` de main, cualquier job sin `environment:`
            # podria desplegar produccion saltandose la aprobacion: la puerta
            # seguiria pintada en la interfaz sin cerrar nada.
            subs=[f"repo:{repo}:environment:prod"],
        )

    # ---------------------------------------------------------- proveedor ---

    def _provider(self) -> iam.IOpenIdConnectProvider:
        """El proveedor OIDC es un recurso UNICO por (cuenta, URL).

        Si ya lo creaste para otro proyecto, crear otro falla con
        EntityAlreadyExists y el stack se queda en ROLLBACK. Y algo peor: si lo
        creas aqui y luego destruyes este stack, te llevas por delante el
        proveedor que usaban los demas repositorios de la cuenta.

        Por eso hay una via de importacion explicita:

            make deploy-cicd OIDC_EXISTENTE=1
        """
        if str(self.node.try_get_context("oidc_existente")).lower() == "true":
            return iam.OpenIdConnectProvider.from_open_id_connect_provider_arn(
                self,
                "GitHubOidc",
                self.format_arn(
                    service="iam",
                    region="",
                    resource="oidc-provider",
                    resource_name=OIDC_HOST,
                ),
            )

        return iam.OpenIdConnectProvider(
            self,
            "GitHubOidc",
            url=f"https://{OIDC_HOST}",
            client_ids=[AUDIENCE],
            # Sin `thumbprints`: desde 2023 AWS valida la cadena de GitHub
            # contra su CA raiz y el campo se ignora. Fijarlo a mano era la
            # causa numero uno de "el CI dejo de funcionar solo": GitHub rotaba
            # el certificado y la huella clavada dejaba de coincidir.
        )

    # --------------------------------------------------------------- rol ---

    def _deploy_role(
        self, nombre: str, provider: iam.IOpenIdConnectProvider, *, subs: list[str]
    ) -> iam.Role:
        principal = iam.WebIdentityPrincipal(
            provider.open_id_connect_provider_arn,
            conditions={
                # `aud`: sin esta condicion aceptarias tokens emitidos para otro
                # publico, por ejemplo los que pide una action de terceros con
                # su propio audience.
                "StringEquals": {f"{OIDC_HOST}:aud": AUDIENCE},
                # `sub`: ESTA es la linea que separa "mi CI despliega" de
                # "internet despliega".
                #
                # El emisor y el `aud` son identicos para TODOS los repositorios
                # de GitHub. Si el trust policy solo comprobara esos dos,
                # cualquiera podria crear un repositorio, copiar el ARN de este
                # rol -que esta escrito en el workflow, y este repo es PUBLICO-,
                # pedir `id-token: write` y entrar en la cuenta. En CloudTrail se
                # veria un AssumeRoleWithWebIdentity perfectamente legitimo.
                #
                # Los valores van enumerados y literales. El comodin que todo el
                # mundo escribe, `repo:owner/repo:*`, incluye los `pull_request`,
                # y el token de un PR se emite contra el repositorio base: un PR
                # desde un fork desplegaria con este rol.
                "StringLike": {f"{OIDC_HOST}:sub": subs},
            },
        )

        role = iam.Role(
            self,
            f"GitHubActions{nombre}",
            role_name=f"practica-github-{nombre.lower()}",
            assumed_by=principal,
            # Un despliegue completo tarda unos 15 minutos. Una hora sobra y
            # acota la ventana si un token se filtrara en un log.
            max_session_duration=Duration.hours(1),
            description=f"Asumido por GitHub Actions para desplegar {nombre.lower()}",
        )

        # Permisos minimos: este rol NO despliega nada por si mismo. Lo unico
        # que sabe hacer es asumir los roles que creo `cdk bootstrap`, que son
        # los que llevan de verdad los permisos sobre CloudFormation, S3 y ECR.
        #
        # La alternativa -colgarle AdministratorAccess- funcionaria igual de
        # bien y convertiria un push malicioso a develop en control total de la
        # cuenta. Asi el radio de accion queda acotado por lo que el bootstrap
        # permite, que es comun a todo el CDK y esta auditado.
        role.add_to_policy(
            iam.PolicyStatement(
                sid="AsumirLosRolesDelBootstrap",
                actions=["sts:AssumeRole"],
                resources=[
                    self.format_arn(
                        service="iam",
                        region="",
                        resource="role",
                        resource_name=(f"cdk-{QUALIFIER}-{rol}-role-{self.account}-{self.region}"),
                    )
                    for rol in BOOTSTRAP_ROLES
                ],
            )
        )

        # El CLI del CDK lee la version del bootstrap de este parametro ANTES de
        # asumir ningun rol. Sin este permiso el despliegue muere con un
        # AccessDenied sobre SSM que no menciona el bootstrap por ningun lado.
        role.add_to_policy(
            iam.PolicyStatement(
                sid="LeerLaVersionDelBootstrap",
                actions=["ssm:GetParameter"],
                resources=[
                    self.format_arn(
                        service="ssm",
                        resource="parameter",
                        resource_name=f"cdk-bootstrap/{QUALIFIER}/version",
                    )
                ],
            )
        )

        CfnOutput(
            self,
            f"RoleArn{nombre}",
            value=role.role_arn,
            description=f"Ponlo en el secret AWS_ROLE_{nombre.upper()}_ARN del repositorio",
        )
        return role
