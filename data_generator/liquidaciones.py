"""Generador de la segunda fuente: ficheros de liquidacion de la pasarela de pago.

El fichero que una distribuidora electrica manda a una comercializadora tiene
una forma muy concreta, heredada de los años ochenta y viva por normativa:
posicional o separado por `;`, codificado en latin-1, con coma decimal, y un
**pie de control** con el numero de registros y las sumas. El fichero diario de
un proveedor de pagos es identico, y por las mismas razones historicas.

    C;LIQ;20260315;001;PSP_ACME
    D;14/03/2026;ORD-000481219;PAGO;125,40;2,81;EUR;VISA
    D;14/03/2026;ORD-000481220;DEVOL;-45,00;-1,01;EUR;MC
    P;000482;123456,78;2765,43

Tres tipos de registro en el mismo fichero, cada uno con su esquema. No es un
CSV: leerlo como tal produce columnas desplazadas y ninguna excepcion.

Lo que se inyecta a proposito, y por que cada cosa es dificil de detectar:

  * **encoding latin-1**: leido como UTF-8 no falla, escribe mojibake;
  * **coma decimal y fecha dd/mm/aaaa**: `03/04/2026` interpretado como
    MM/dd/yyyy es abril en vez de marzo, y NUNCA da error;
  * **pie que no cuadra**: el unico modo de saber que el fichero llego a
    medias, porque un fichero truncado se lee perfectamente;
  * **fichero reenviado con otro nombre**: duplica la liquidacion de un dia si
    la idempotencia se apoya en el nombre en vez de en el contenido;
  * **hueco en la secuencia**: un dia sin liquidar no se distingue de un dia
    sin ventas si nadie mira los numeros de fichero;
  * **devoluciones**: importes negativos CORRECTOS, que la regla `non_negative`
    de las otras tablas mandaria a cuarentena.

Uso:
    make liquidaciones
"""

from __future__ import annotations

import argparse
import logging
import random
import shutil
from dataclasses import dataclass, replace
from datetime import date, timedelta
from pathlib import Path

from common.liquidaciones import (
    CABECERA,
    CODIFICACION,
    DETALLE,
    PIE,
    SEPARADOR,
    nombre_de_fichero,
)
from data_generator.seed import connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
log = logging.getLogger("liquidaciones")

PROVEEDOR = "PSP_ACME"
TARJETAS = ["VISA", "MC", "AMEX", "BIZUM"]

# Comision del proveedor: un fijo mas un porcentaje. Es la razon por la que el
# importe liquidado NUNCA coincide con el total del pedido, y por tanto la
# razon de ser de la conciliacion.
COMISION_FIJA = 0.25
COMISION_PORCENTAJE = 0.019


@dataclass(frozen=True)
class FileDirtRates:
    """Modos de fallo de una integracion por ficheros.

    Ninguno es "un dato mal tecleado": son formas de que un fichero entero
    mienta, que es lo que distingue esta clase de origen de una tabla.
    """

    pie_descuadrado: float = 0.04
    """El pie declara un total que no coincide con los detalles. Es lo unico
    que delata un fichero truncado: por dentro se lee perfectamente."""

    reenviado: float = 0.05
    """El mismo contenido, otro nombre. Duplica la liquidacion del dia si la
    idempotencia mira el nombre en vez del contenido."""

    fichero_perdido: float = 0.03
    """Un dia que no llega. Sin comprobar la secuencia, no se distingue de un
    dia sin ventas."""

    acentos: float = 0.30
    """Proporcion de detalles con un concepto acentuado. No es un fallo: es lo
    que hace visible el problema de codificacion."""

    @classmethod
    def scaled(cls, factor: float) -> FileDirtRates:
        base = cls()
        return replace(
            base, **{c: min(1.0, getattr(base, c) * factor) for c in base.__dataclass_fields__}
        )


CONCEPTOS = [
    "Compra online",
    "Devolucion parcial",
    "Cargo por logistica",
    "Suscripcion mensual",
    # Con acentos y ene: son los que revientan si se lee el fichero como UTF-8.
    "Compra en periodo de rebajas",
    "Reembolso por articulo dañado",
    "Envio urgente a la peninsula",
    "Devolucion por garantia",
]


def _importe(valor: float) -> str:
    """Formato español: coma decimal y sin separador de miles."""
    return f"{valor:.2f}".replace(".", ",")


def _fecha(dia: date) -> str:
    """dd/mm/aaaa. El formato que se lee mal sin dar error."""
    return dia.strftime("%d/%m/%Y")


def pedidos_del_dia(conn, dia: date) -> list[tuple[int, float]]:
    """Los pedidos cobrados ese dia, que son los que el proveedor liquida."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT order_id, total_amount
            FROM ecommerce.orders
            WHERE order_date = %s AND status IN ('paid', 'shipped', 'delivered', 'returned')
              AND total_amount IS NOT NULL AND total_amount > 0
            ORDER BY order_id
            """,
            (dia,),
        )
        return [(o, float(t)) for o, t in cur.fetchall()]


def construir_fichero(
    dia: date, secuencia: int, pedidos: list[tuple[int, float]], dirt: FileDirtRates
) -> tuple[str, float, float]:
    """Devuelve (contenido, suma_importes, suma_comisiones).

    El pie se calcula sobre lo que de verdad se ha escrito, y solo despues se
    estropea si toca. Al reves seria imposible saber si un descuadre es el
    inyectado o un error del generador.
    """
    lineas = [
        SEPARADOR.join([CABECERA, "LIQ", dia.strftime("%Y%m%d"), f"{secuencia:03d}", PROVEEDOR])
    ]

    total_importe = 0.0
    total_comision = 0.0
    detalles = 0

    for order_id, importe_pedido in pedidos:
        devolucion = random.random() < 0.06
        importe = -round(importe_pedido, 2) if devolucion else round(importe_pedido, 2)
        comision = round(COMISION_FIJA + abs(importe) * COMISION_PORCENTAJE, 2)
        if devolucion:
            comision = -comision

        concepto = random.choice(CONCEPTOS if random.random() < dirt.acentos else CONCEPTOS[:4])

        lineas.append(
            SEPARADOR.join(
                [
                    DETALLE,
                    _fecha(dia),
                    # La referencia del proveedor NO es el order_id: lleva
                    # prefijo y ceros a la izquierda. Hay que normalizarla para
                    # poder cruzarla, y ahi es donde se pierden filas en
                    # silencio si nadie se fija.
                    f"ORD-{order_id:09d}",
                    "DEVOL" if devolucion else "PAGO",
                    _importe(importe),
                    _importe(comision),
                    "EUR",
                    random.choice(TARJETAS),
                    concepto,
                ]
            )
        )
        total_importe += importe
        total_comision += comision
        detalles += 1

    total_importe = round(total_importe, 2)
    total_comision = round(total_comision, 2)

    declarados = (detalles, total_importe, total_comision)
    if random.random() < dirt.pie_descuadrado:
        # Un fichero que llego a medias: el pie sigue diciendo lo que el emisor
        # queria mandar. Es la unica forma de enterarse.
        recorte = random.randint(1, max(1, detalles // 5))
        lineas = lineas[: len(lineas) - recorte]
        log.warning("  fichero del %s truncado en %d detalles (el pie no cuadrara)", dia, recorte)

    lineas.append(
        SEPARADOR.join(
            [PIE, f"{declarados[0]:06d}", _importe(declarados[1]), _importe(declarados[2])]
        )
    )
    return "\n".join(lineas) + "\n", total_importe, total_comision


def escribir(destino: Path, nombre: str, contenido: str) -> None:
    """Escribe en latin-1, que es como llegan estos ficheros de verdad.

    Un fichero asi leido como UTF-8 no lanza excepcion con la configuracion por
    defecto de Spark: sustituye los bytes que no entiende y sigue. El acento se
    convierte en un simbolo raro y viaja hasta Gold sin que nadie lo pare.
    """
    (destino / nombre).write_text(contenido, encoding=CODIFICACION)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--output", default="data/landing/liquidaciones")
    p.add_argument("--days", type=int, default=45, help="dias de liquidacion a generar")
    p.add_argument("--dirt-factor", type=float, default=1.0)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)
    dirt = FileDirtRates.scaled(args.dirt_factor)

    destino = Path(args.output).resolve()
    if destino.exists():
        shutil.rmtree(destino)
    destino.mkdir(parents=True)

    conn = connect()
    try:
        # El proveedor liquida con un dia de retraso, asi que se empieza por
        # ayer y se va hacia atras.
        ultimo = date.today() - timedelta(days=1)
        escritos = 0
        perdidos = 0
        reenviados = 0
        suma = 0.0

        for i in range(args.days):
            dia = ultimo - timedelta(days=i)
            secuencia = args.days - i

            if random.random() < dirt.fichero_perdido:
                log.warning(
                    "  el fichero %03d (%s) no llega: hueco en la secuencia", secuencia, dia
                )
                perdidos += 1
                continue

            pedidos = pedidos_del_dia(conn, dia)
            if not pedidos:
                continue

            contenido, importe, _ = construir_fichero(dia, secuencia, pedidos, dirt)
            escribir(destino, nombre_de_fichero(dia, secuencia), contenido)
            escritos += 1
            suma += importe

            if random.random() < dirt.reenviado:
                # Mismo contenido byte a byte, otro nombre. Solo un hash lo caza.
                escribir(destino, nombre_de_fichero(dia, secuencia, reenvio=True), contenido)
                reenviados += 1
                log.warning("  el fichero %03d (%s) se reenvia con otro nombre", secuencia, dia)

        log.info("--- ficheros de liquidacion ---")
        log.info("escritos:   %d en %s", escritos, destino)
        log.info("reenviados: %d (mismo contenido, otro nombre)", reenviados)
        log.info("perdidos:   %d (huecos en la secuencia)", perdidos)
        log.info("importe declarado total: %s EUR", _importe(round(suma, 2)))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
