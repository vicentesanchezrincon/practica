"""Watermarks de la extraccion incremental.

Un watermark es la respuesta a "¿hasta donde lei la ultima vez?". Se guarda uno
por tabla, y la siguiente ejecucion solo pide al origen las filas cuyo
`updated_at` sea posterior. Sin esto, cada ejecucion se traeria la tabla entera.

Se guardan en **SSM Parameter Store** y no en un fichero en S3 porque:

  * es gratis en el tier estandar,
  * es transaccional: no existe el estado "medio escrito",
  * se puede consultar y corregir con un comando, que es justo lo que necesitas
    cuando tengas que reprocesar un dia.

Reprocesar es entonces tan simple como retrasar el watermark:

    aws ssm put-parameter --overwrite \\
        --name /practica/dev/watermark/orders \\
        --value 2026-01-01T00:00:00+00:00 --type String
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

# Punto de partida cuando una tabla no se ha ingestado nunca. Con esto, la
# primera ejecucion se trae el historico completo sin necesitar un modo aparte.
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def parameter_name(table: str, environment: str) -> str:
    return f"/practica/{environment}/watermark/{table}"


class WatermarkStore:
    """Lee y escribe watermarks. Una instancia por ejecucion del job."""

    def __init__(self, environment: str, client=None) -> None:
        self.environment = environment
        self._client = client or boto3.client("ssm")

    def read(self, table: str) -> datetime:
        """Ultimo `updated_at` procesado. EPOCH si la tabla es nueva."""
        name = parameter_name(table, self.environment)
        try:
            valor = self._client.get_parameter(Name=name)["Parameter"]["Value"]
        except ClientError as error:
            if error.response["Error"]["Code"] == "ParameterNotFound":
                log.info("%s sin watermark previo: carga completa", table)
                return EPOCH
            raise
        return parse(valor)

    def write(self, table: str, value: datetime) -> None:
        """Guarda el watermark. Solo debe llamarse si la escritura fue bien.

        Si el job falla despues de escribir en S3 pero antes de esto, la
        siguiente ejecucion volvera a traerse las mismas filas. Eso es
        preferible al caso contrario: Bronze es append-only y Silver deduplica,
        asi que repetir es inofensivo, mientras que perder filas es un agujero
        silencioso en los datos.
        """
        self._client.put_parameter(
            Name=parameter_name(table, self.environment),
            Value=format(value),
            Type="String",
            Overwrite=True,
            Description=f"Ultimo updated_at ingestado de {table}",
        )
        log.info("%s watermark -> %s", table, format(value))


def parse(value: str) -> datetime:
    """Texto ISO-8601 a datetime con zona horaria."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        # Un watermark sin zona horaria es ambiguo y acaba provocando saltos de
        # una hora al cambiar el horario de verano. Asumimos UTC.
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def format(value: datetime) -> str:  # noqa: A001
    """datetime a texto ISO-8601 en UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()
