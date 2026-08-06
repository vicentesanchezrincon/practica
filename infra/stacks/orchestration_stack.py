"""Orquestacion: la maquina de estados que ejecuta el pipeline entero.

    bronze-ingest ──► silver-transform ──► ¿calidad? ──no──► SNS + Fail
                                              │ si
                                              ▼
                                          gold-build ──► Success

Dos decisiones que se apartan del plan original, con su motivo:

**No hay estado Map sobre las tablas.** El plan proponia un `Map` con
concurrencia 3 que lanzara un job por tabla. No compensa: `bronze-ingest` ya
recorre las cuatro tablas dentro de una sola ejecucion y Spark paraleliza por
dentro. Un Map serian cuatro arranques de Glue (~1 minuto cada uno solo de
arrancar) en lugar de uno, mas caro y mas lento, a cambio de nada. Map tiene
sentido cuando cada elemento necesita su propio cluster o cuando quieres que el
fallo de uno no bloquee a los demas; aqui no es el caso.

**La puerta de calidad la aplica esta maquina, no el job.** El job de Silver
escribe su informe en S3 y no decide nada. Asi el criterio vive en un solo
sitio (`config.py`) y solo hay un sitio que lo aplique. Ademas permite
distinguir dos cosas que no son iguales: que el pipeline se pare porque los
datos venian mal (flujo controlado, con su mensaje) y que se pare porque un job
reviento (el `Catch`).

El informe se lee con la integracion SDK de Step Functions contra S3, sin
Lambda de por medio.
"""

from __future__ import annotations

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_glue as glue
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct

QUALITY_REPORT_KEY = "_quality/silver/latest.json"


class OrchestrationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        environment: str,
        bucket: s3.IBucket,
        bronze_job: glue.CfnJob,
        silver_job: glue.CfnJob,
        gold_job: glue.CfnJob,
        alert_email: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.environment_name = environment
        self.topic = self._create_topic(alert_email)

        definicion = self._build_definition(bucket, bronze_job, silver_job, gold_job)

        self.state_machine = sfn.StateMachine(
            self,
            "Pipeline",
            state_machine_name=f"practica-{environment}-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definicion),
            # Un pipeline que tarda mas de dos horas es que algo va mal; mejor
            # que se corte a que se quede colgado consumiendo.
            timeout=Duration.hours(2),
            comment="Bronze -> Silver -> (calidad) -> Gold",
        )

        self.topic.grant_publish(self.state_machine)
        bucket.grant_read(self.state_machine)

        self._create_schedule()
        self._outputs()

    # ------------------------------------------------------------------ SNS ---

    def _create_topic(self, alert_email: str | None) -> sns.Topic:
        topic = sns.Topic(
            self,
            "Alerts",
            topic_name=f"practica-{self.environment_name}-alerts",
            display_name="Alertas del pipeline de practica",
        )
        if alert_email:
            # AWS manda un correo de confirmacion: hasta que no lo aceptes, la
            # suscripcion queda pendiente y no llega ninguna alerta.
            topic.add_subscription(subs.EmailSubscription(alert_email))
        return topic

    # -------------------------------------------------------------- estados ---

    def _glue_task(
        self, construct_id: str, job: glue.CfnJob, comment: str
    ) -> tasks.GlueStartJobRun:
        """Lanza un job de Glue y ESPERA a que termine.

        `RUN_JOB` es lo que hace que espere. Con `REQUEST_RESPONSE` la maquina
        seguiria al estado siguiente nada mas lanzar el job, y Silver empezaria
        con Bronze a medias.
        """
        # Los permisos (StartJobRun, GetJobRun y BatchStopJobRun) los concede el
        # propio constructo. BatchStopJobRun es el que se olvida a mano: sin el,
        # la maquina lanza el job pero no puede pararlo si cancelas la ejecucion.
        return tasks.GlueStartJobRun(
            self,
            construct_id,
            glue_job_name=job.ref,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            comment=comment,
            result_path=sfn.JsonPath.DISCARD,
        ).add_retry(
            # Los fallos transitorios en una VPC existen. El backoff evita
            # insistir contra un servicio que ya esta teniendo problemas.
            errors=["States.TaskFailed"],
            interval=Duration.seconds(30),
            max_attempts=2,
            backoff_rate=2.0,
        )

    def _build_definition(
        self,
        bucket: s3.IBucket,
        bronze_job: glue.CfnJob,
        silver_job: glue.CfnJob,
        gold_job: glue.CfnJob,
    ) -> sfn.IChainable:
        bronze = self._glue_task("Bronze", bronze_job, "Extraccion incremental del RDS")
        silver = self._glue_task("Silver", silver_job, "Limpieza, cuarentena y MERGE")
        gold = self._glue_task("Gold", gold_job, "Modelo estrella")

        # Lee el informe de calidad de S3 con la integracion SDK, sin Lambda.
        leer_informe = tasks.CallAwsService(
            self,
            "LeerInformeDeCalidad",
            service="s3",
            action="getObject",
            parameters={"Bucket": bucket.bucket_name, "Key": QUALITY_REPORT_KEY},
            iam_resources=[bucket.arn_for_objects(QUALITY_REPORT_KEY)],
            result_selector={
                # El cuerpo llega como texto; hay que parsearlo para poder
                # consultar un campo concreto en el Choice.
                "informe": sfn.JsonPath.string_to_json(sfn.JsonPath.string_at("$.Body"))
            },
            result_path="$.calidad",
        )

        alerta_calidad = tasks.SnsPublish(
            self,
            "AvisarCalidad",
            topic=self.topic,
            subject=f"[practica-{self.environment_name}] Pipeline detenido por calidad",
            # SNS exige que Message sea un STRING. Pasar "$.calidad.informe" a
            # secas manda un objeto y la publicacion falla en ejecucion, no en
            # el synth: es de los errores que solo aparecen desplegando.
            message=sfn.TaskInput.from_text(
                sfn.JsonPath.json_to_string(sfn.JsonPath.object_at("$.calidad.informe"))
            ),
            result_path=sfn.JsonPath.DISCARD,
        )

        alerta_fallo = tasks.SnsPublish(
            self,
            "AvisarFallo",
            topic=self.topic,
            subject=f"[practica-{self.environment_name}] Pipeline fallido",
            message=sfn.TaskInput.from_text(
                sfn.JsonPath.json_to_string(sfn.JsonPath.object_at("$.error"))
            ),
            result_path=sfn.JsonPath.DISCARD,
        )

        parada_por_calidad = sfn.Fail(
            self,
            "CalidadInsuficiente",
            cause="Se rechazaron demasiadas filas; Gold no se construye sobre este lote",
            error="CalidadInsuficiente",
        )

        fallo = sfn.Fail(self, "PipelineFallido", error="PipelineFallido")

        puerta = (
            sfn.Choice(self, "PuertaDeCalidad", comment="Solo se publica Gold si la calidad pasa")
            .when(
                sfn.Condition.boolean_equals("$.calidad.informe.passed", True),
                gold.next(sfn.Succeed(self, "Completado")),
            )
            .otherwise(alerta_calidad.next(parada_por_calidad))
        )

        # Cualquier fallo no controlado avisa y termina. Sin esto, un job que
        # revienta deja la ejecucion en rojo y nadie se entera hasta que alguien
        # mira la consola.
        manejo_de_errores = alerta_fallo.next(fallo)
        for estado in (bronze, silver, leer_informe):
            estado.add_catch(manejo_de_errores, errors=["States.ALL"], result_path="$.error")
        gold.add_catch(manejo_de_errores, errors=["States.ALL"], result_path="$.error")

        return bronze.next(silver).next(leer_informe).next(puerta)

    # ------------------------------------------------------------ programado ---

    def _create_schedule(self) -> None:
        """Ejecucion diaria.

        Deshabilitada en dev: no queremos que se despierte sola a las 3 de la
        manana contra una infraestructura que probablemente ya has destruido.
        """
        es_prod = self.environment_name == "prod"

        self.rule = events.Rule(
            self,
            "DailyRun",
            rule_name=f"practica-{self.environment_name}-daily",
            description="Ejecuta el pipeline todos los dias de madrugada",
            schedule=events.Schedule.cron(minute="0", hour="3"),
            enabled=es_prod,
            targets=[targets.SfnStateMachine(self.state_machine)],
        )

    # --------------------------------------------------------------- outputs ---

    def _outputs(self) -> None:
        CfnOutput(self, "StateMachineArn", value=self.state_machine.state_machine_arn)
        CfnOutput(self, "AlertTopicArn", value=self.topic.topic_arn)
        CfnOutput(
            self,
            "LanzarPipeline",
            value=f"aws stepfunctions start-execution --state-machine-arn {self.state_machine.state_machine_arn}",
            description="Comando para ejecutar el pipeline completo",
        )
