"""Tests del watermark de la extraccion incremental.

Sin Spark ni AWS: el cliente de SSM se sustituye por un doble. Lo que se fija
aqui es la logica que, si falla, provoca el peor bug posible en un pipeline —
perder filas en silencio.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from botocore.exceptions import ClientError

from common.watermark import EPOCH, WatermarkStore, format, parameter_name, parse


class FakeSsm:
    """Doble del cliente de SSM. Guarda los parametros en un dict."""

    def __init__(self, initial: dict[str, str] | None = None) -> None:
        self.params = dict(initial or {})
        self.puts: list[tuple[str, str]] = []

    def get_parameter(self, Name: str):  # noqa: N803  (la API de boto3 usa PascalCase)
        if Name not in self.params:
            raise ClientError(
                {"Error": {"Code": "ParameterNotFound", "Message": "no existe"}},
                "GetParameter",
            )
        return {"Parameter": {"Value": self.params[Name]}}

    def put_parameter(self, Name: str, Value: str, **kwargs):  # noqa: N803
        self.params[Name] = Value
        self.puts.append((Name, Value))


@pytest.fixture
def ssm():
    return FakeSsm()


# ------------------------------------------------------------- formato ------


def test_el_nombre_del_parametro_separa_entornos():
    assert parameter_name("orders", "dev") != parameter_name("orders", "prod")
    assert parameter_name("orders", "dev") == "/practica/dev/watermark/orders"


def test_ida_y_vuelta_conserva_el_instante():
    momento = datetime(2026, 3, 15, 14, 30, 45, tzinfo=UTC)
    assert parse(format(momento)) == momento


def test_un_watermark_sin_zona_horaria_se_asume_utc():
    """Un watermark ambiguo provoca saltos de una hora al cambiar el horario de
    verano, y eso son filas perdidas o reprocesadas sin motivo."""
    assert parse("2026-03-15T14:30:45").tzinfo is not None
    assert parse("2026-03-15T14:30:45") == parse("2026-03-15T14:30:45+00:00")


def test_se_normaliza_a_utc_desde_otro_huso():
    madrid = parse("2026-03-15T15:30:45+01:00")
    assert madrid == datetime(2026, 3, 15, 14, 30, 45, tzinfo=UTC)


# -------------------------------------------------------------- lectura ----


def test_una_tabla_nueva_arranca_en_epoch(ssm):
    """Sin caso especial para la primera carga: el epoch hace que la primera
    ejecucion se traiga el historico completo."""
    store = WatermarkStore("dev", client=ssm)
    assert store.read("customers") == EPOCH


def test_se_lee_el_watermark_guardado(ssm):
    ssm.params["/practica/dev/watermark/orders"] = "2026-03-15T14:30:45+00:00"
    store = WatermarkStore("dev", client=ssm)
    assert store.read("orders") == datetime(2026, 3, 15, 14, 30, 45, tzinfo=UTC)


def test_un_error_de_ssm_que_no_sea_parametro_inexistente_se_propaga(ssm):
    """Si SSM responde 'acceso denegado' y lo tratamos como 'no hay watermark',
    el job se traeria el historico entero creyendo que es la primera vez."""

    def boom(Name):  # noqa: N803
        raise ClientError({"Error": {"Code": "AccessDeniedException"}}, "GetParameter")

    ssm.get_parameter = boom
    store = WatermarkStore("dev", client=ssm)
    with pytest.raises(ClientError):
        store.read("orders")


# ------------------------------------------------------------- escritura ---


def test_escribir_y_releer(ssm):
    store = WatermarkStore("dev", client=ssm)
    momento = datetime(2026, 3, 15, 14, 30, 45, tzinfo=UTC)
    store.write("orders", momento)
    assert store.read("orders") == momento


def test_los_entornos_no_se_pisan(ssm):
    dev = WatermarkStore("dev", client=ssm)
    prod = WatermarkStore("prod", client=ssm)
    ayer = datetime(2026, 3, 14, tzinfo=UTC)
    hoy = ayer + timedelta(days=1)

    dev.write("orders", ayer)
    prod.write("orders", hoy)

    assert dev.read("orders") == ayer
    assert prod.read("orders") == hoy


def test_el_watermark_solo_se_escribe_cuando_se_lo_pedimos(ssm):
    """Leer no debe tener efectos secundarios: si `read` guardara algo, un job
    que falla a mitad dejaria el watermark avanzado y perderiamos esas filas."""
    store = WatermarkStore("dev", client=ssm)
    store.read("orders")
    store.read("customers")
    assert ssm.puts == []
