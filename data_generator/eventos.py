"""Generador de la serie temporal: eventos de la tienda web.

Es la segunda fuente del proyecto y no se parece en nada a la primera. Las
cuatro tablas de `seed.py` son entidades que se actualizan; esto es un flujo de
hechos que solo crece, con la forma que tiene cualquier historian del sector
energetico: una medida, un instante, y nunca se toca.

La suciedad que inyecta aqui no es "datos mal tecleados". Son los cinco modos
de fallo propios de un flujo de eventos, y ninguno de ellos existe en una
extraccion JDBC de una tabla maestra:

  1. **Reenvios.** La entrega es "al menos una vez": la red se corta despues de
     escribir y antes de confirmar, el cliente reintenta, el evento entra dos
     veces con el MISMO event_id.
  2. **Eventos tardios.** Un movil sin cobertura acumula y sincroniza horas
     despues. Su `received_at` va muy por detras de su `event_time`, y quien
     use la columna equivocada para el incremental los pierde todos.
  3. **Relojes desviados.** El `event_time` lo pone el dispositivo del
     visitante, no el servidor. Los hay adelantados, y alguno manda eventos
     "del futuro".
  4. **Deriva de esquema.** Cada version de la web mete campos distintos en
     `properties`. No es un error: es la vida de una carga semiestructurada.
  5. **Vocabulario nuevo.** Aparece un `event_type` que nadie declaro, porque
     alguien lanzo una funcionalidad y no aviso.

Y una trampa que no es suciedad, sino calendario: **el cambio de hora**. El
historico cubre las dos transiciones, y en ellas la hora local miente. Ver
`--history-days`.

Uso:
    make events          # equivale a --mode initial
    make events-daily    # el trafico de un dia mas
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg

from data_generator.seed import connect, copy_rows, fetch_ids

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
log = logging.getLogger("eventos")

MADRID = ZoneInfo("Europe/Madrid")

DEVICES = ["movil", "escritorio", "tablet"]
UTM_SOURCES = ["organico", "google_ads", "newsletter", "instagram", "afiliados", "directo"]

# Tipos que la web emite pero el pipeline todavia no conoce. Son el ensayo de
# "alguien lanzo una funcionalidad y no aviso a datos".
TIPOS_NO_DECLARADOS = ["wishlist_add", "compartir_producto", "chat_abierto"]

EVENT_COLS = [
    "event_id",
    "event_time",
    "received_at",
    "session_id",
    "customer_id",
    "event_type",
    "product_id",
    "quantity",
    "amount",
    "device",
    "utm_source",
    "properties",
]


@dataclass(frozen=True)
class EventDirtRates:
    """Con que frecuencia aparece cada modo de fallo.

    Los valores por defecto estan calibrados para que las reglas de calidad
    tengan algo que hacer sin que la cuarentena se dispare. `scaled` permite
    subirlos todos a la vez para ver que pasa cuando el origen se degrada.
    """

    reenvio: float = 0.02
    """Mismo event_id, dos veces. Es lo que obliga a deduplicar en Silver."""

    tardio: float = 0.03
    """received_at muy posterior a event_time. Sin ventana de reproceso, se pierden."""

    reloj_adelantado: float = 0.01
    """event_time por delante de received_at: imposible, y solo lo ve time_sanity."""

    tipo_no_declarado: float = 0.004
    """Un event_type que no esta en el vocabulario acordado."""

    producto_huerfano: float = 0.006
    """Referencia a un product_id que no existe."""

    cantidad_absurda: float = 0.002
    """Un carrito de miles de unidades. Casi siempre es un bot o un test."""

    @classmethod
    def scaled(cls, factor: float) -> EventDirtRates:
        base = cls()
        return replace(
            base,
            **{campo: getattr(base, campo) * factor for campo in base.__dataclass_fields__},
        )


# Proporcion de sesiones sin identificar. NO es suciedad: la mayoria del
# trafico de una tienda no ha iniciado sesion, y esos eventos son datos
# perfectamente buenos. Esta aqui, y no en EventDirtRates, justamente para que
# no se confunda con un defecto.
TASA_ANONIMA = 0.62


# Pesos por hora local. El trafico de una tienda no es uniforme: valle de
# madrugada y dos picos, mediodia y noche.
PESOS_POR_HORA = [2, 1, 1, 1, 1, 2, 4, 8, 12, 14, 15, 16, 18, 16, 14, 14, 15, 17, 20, 22, 18, 12, 7, 4]  # fmt: skip


def _hora_realista(dia_inicial: date, dias: int) -> datetime:
    """Un instante dentro del historico, con forma de dia real.

    Se genera en hora **local de Madrid** y se convierte a UTC, que es como
    llega de verdad y como hay que guardarlo. Ese paso es el que introduce el
    cambio de hora sin que haya que simularlo:

      * el ultimo domingo de octubre, la hora local 02:00-03:00 ocurre DOS
        veces, con una hora de diferencia en UTC. Python lo distingue con
        `fold`, y aqui se elige al azar para que existan las dos. No son
        duplicados, y deduplicar por hora local borraria la mitad.
      * el ultimo domingo de marzo, esa misma hora local NO existe. Un evento
        con esa marca es imposible, y aun asi `astimezone` devuelve algo.
    """
    dia = dia_inicial + timedelta(days=random.randint(0, dias))
    hora = random.choices(range(24), weights=PESOS_POR_HORA, k=1)[0]
    local = datetime(
        dia.year,
        dia.month,
        dia.day,
        hora,
        random.randint(0, 59),
        random.randint(0, 59),
        tzinfo=MADRID,
    )
    # Hora ambigua (la que se repite al atrasar el reloj): las dos lecturas son
    # instantes UTC reales y distintos, asi que se sortea cual de las dos.
    if local.utcoffset() != local.replace(fold=1).utcoffset():
        local = local.replace(fold=random.randint(0, 1))
    return local.astimezone(UTC)


def _propiedades(tipo: str) -> str:
    """Carga semiestructurada, con esquema deliberadamente inestable.

    Tres versiones de la web conviven en produccion durante semanas: la vieja
    manda `ab_test`, la nueva `experimento`, y ninguna manda todo siempre. Quien
    aplane esto a columnas fijas descubrira que un tercio son nulos.
    """
    props: dict[str, object] = {"pagina": random.choice(["home", "categoria", "ficha", "carrito"])}
    if random.random() < 0.5:
        props["ab_test"] = random.choice(["A", "B"])
    else:
        props["experimento"] = random.choice(["control", "variante_1", "variante_2"])
    if random.random() < 0.3:
        props["referrer"] = random.choice(["google.com", "instagram.com", "", None])
    if tipo == "purchase" and random.random() < 0.7:
        props["metodo_pago"] = random.choice(["tarjeta", "paypal", "bizum", "transferencia"])
    return json.dumps(props, ensure_ascii=False)


def _sesion(
    customer_ids: list[int],
    product_ids: list[int],
    max_product_id: int,
    dirt: EventDirtRates,
    dia_inicial: date,
    dias: int,
    inicio: datetime | None = None,
) -> list[tuple]:
    """Una visita completa, con forma de embudo.

    El embudo es el que se veria en cualquier tienda: casi todo el mundo mira,
    bastantes menos anaden al carrito, y una minoria compra. Los eventos de una
    sesion comparten `session_id` y se separan pocos minutos entre si, que es
    lo que despues permite sesionizar por inactividad.
    """
    session_id = uuid.uuid4().hex[:16]
    anonimo = random.random() < TASA_ANONIMA
    customer_id = None if anonimo else random.choice(customer_ids)
    device = random.choice(DEVICES)
    utm = random.choice(UTM_SOURCES)

    eventos: list[tuple] = []
    reloj = inicio if inicio is not None else _hora_realista(dia_inicial, dias)

    def emitir(tipo: str, product_id=None, quantity=None, amount=None) -> None:
        nonlocal reloj
        reloj = reloj + timedelta(seconds=random.randint(5, 240))
        event_time = reloj

        # received_at: normalmente unos segundos despues del evento.
        received_at = event_time + timedelta(seconds=random.randint(0, 8))
        if random.random() < dirt.tardio:
            # El movil estaba sin cobertura y sincroniza horas mas tarde.
            received_at = event_time + timedelta(hours=random.randint(2, 30))
        if random.random() < dirt.reloj_adelantado:
            # Reloj del dispositivo adelantado: el evento dice haber ocurrido
            # despues de haber llegado, que es imposible.
            event_time = received_at + timedelta(minutes=random.randint(5, 600))

        if random.random() < dirt.tipo_no_declarado:
            tipo = random.choice(TIPOS_NO_DECLARADOS)
        if product_id is not None and random.random() < dirt.producto_huerfano:
            product_id = max_product_id + random.randint(10_000, 99_999)
        if quantity is not None and random.random() < dirt.cantidad_absurda:
            quantity = random.randint(5_000, 90_000)

        fila = (
            str(uuid.uuid4()),
            event_time,
            received_at,
            session_id,
            customer_id,
            tipo,
            product_id,
            quantity,
            amount,
            device,
            utm,
            _propiedades(tipo),
        )
        eventos.append(fila)

        # Reenvio: MISMO event_id, otro received_at. Es lo que hace que Silver
        # tenga que deduplicar de verdad, y lo que un PRIMARY KEY habria
        # escondido resolviendo el problema en el origen.
        if random.random() < dirt.reenvio:
            eventos.append((fila[0], fila[1], fila[2] + timedelta(seconds=30), *fila[3:]))

    emitir("page_view")
    for _ in range(random.randint(1, 6)):
        emitir("product_view", product_id=random.choice(product_ids))

    if random.random() < 0.28:
        producto = random.choice(product_ids)
        cantidad = random.randint(1, 4)
        emitir("add_to_cart", product_id=producto, quantity=cantidad)
        if random.random() < 0.15:
            emitir("remove_from_cart", product_id=producto, quantity=cantidad)
        elif random.random() < 0.45:
            emitir("checkout_start")
            if random.random() < 0.62:
                emitir(
                    "purchase",
                    product_id=producto,
                    quantity=cantidad,
                    amount=round(random.uniform(8.0, 1_200.0), 2),
                )

    return eventos


def transiciones_horarias(desde: date, hasta: date) -> list[tuple[date, str]]:
    """Los dias del historico en que cambia la hora, y en que sentido.

    Se detectan comparando el desfase UTC al principio y al final del dia en
    lugar de codificar "el ultimo domingo de marzo": la regla cambia, ya cambio
    en el pasado y hay una directiva europea para suprimirla. Preguntarselo a
    la libreria de husos es lo unico que no caduca.
    """
    dias: list[tuple[date, str]] = []
    dia = desde
    while dia <= hasta:
        inicio = datetime(dia.year, dia.month, dia.day, 0, 30, tzinfo=MADRID).utcoffset()
        final = datetime(dia.year, dia.month, dia.day, 23, 30, tzinfo=MADRID).utcoffset()
        if inicio != final:
            dias.append((dia, "adelanta" if final > inicio else "atrasa"))
        dia += timedelta(days=1)
    return dias


def _sesiones_del_cambio_de_hora(
    customer_ids: list[int],
    product_ids: list[int],
    max_product_id: int,
    dirt: EventDirtRates,
    desde: date,
    dias: int,
    por_transicion: int,
) -> list[tuple]:
    """Trafico garantizado en las horas donde el reloj local miente.

    Sin esto la trampa no aparece: repartidos por todo un ano, a la hora de
    madrugada en que se cambia la hora le tocan menos de un evento. Lo que se
    quiere practicar hay que ponerlo a proposito, igual que `seed.py` inyecta
    emails rotos en vez de esperar a que salgan solos.

    Se generan las dos situaciones:

      * **Se atrasa** (octubre): 02:00-02:59 local ocurre DOS veces, separadas
        por una hora real. Son instantes distintos y eventos distintos. Quien
        deduplique por hora local se come la mitad.
      * **Se adelanta** (marzo): 02:00-02:59 local NO existe. Un evento con esa
        marca es imposible, y aun asi la conversion devuelve algo.
    """
    hasta = desde + timedelta(days=dias)
    filas: list[tuple] = []

    for dia, sentido in transiciones_horarias(desde, hasta):
        folds = (0, 1) if sentido == "atrasa" else (0,)
        for fold in folds:
            for _ in range(por_transicion):
                local = datetime(
                    dia.year,
                    dia.month,
                    dia.day,
                    2,
                    random.randint(0, 59),
                    random.randint(0, 59),
                    tzinfo=MADRID,
                    fold=fold,
                )
                filas.extend(
                    _sesion(
                        customer_ids,
                        product_ids,
                        max_product_id,
                        dirt,
                        dia,
                        0,
                        inicio=local.astimezone(UTC),
                    )
                )
        log.info("  cambio de hora el %s (el reloj se %s)", dia, sentido)

    return filas


def generar(
    conn: psycopg.Connection,
    n_sesiones: int,
    dias: int,
    dirt: EventDirtRates,
) -> list[tuple]:
    customer_ids = fetch_ids(conn, "customers", "customer_id")
    product_ids = fetch_ids(conn, "products", "product_id")
    if not customer_ids or not product_ids:
        raise SystemExit("No hay clientes ni productos. Ejecuta antes 'make seed'.")

    max_product_id = max(product_ids)
    dia_inicial = (datetime.now(UTC) - timedelta(days=dias)).date()

    filas: list[tuple] = []
    for _ in range(n_sesiones):
        filas.extend(_sesion(customer_ids, product_ids, max_product_id, dirt, dia_inicial, dias))

    filas.extend(
        _sesiones_del_cambio_de_hora(
            customer_ids,
            product_ids,
            max_product_id,
            dirt,
            dia_inicial,
            dias,
            por_transicion=max(20, n_sesiones // 200),
        )
    )
    return filas


def resumen(conn: psycopg.Connection) -> None:
    """Lo que se imprime aqui esta elegido para que se vea la trampa.

    Las dos ultimas cifras son la razon de ser de esta fuente: hay eventos cuyo
    retraso se mide en horas, y hay una hora local que aparece dos veces en el
    calendario.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), min(event_time), max(event_time) FROM analytics.web_events")
        total, desde, hasta = cur.fetchone()
        log.info("web_events    %8d filas   de %s a %s", total, desde, hasta)

        cur.execute("""
            SELECT count(*) FROM analytics.web_events
            WHERE received_at - event_time > INTERVAL '1 hour'
        """)
        log.info("  tardios (mas de 1h de retraso): %d", cur.fetchone()[0])

        cur.execute("""
            SELECT count(*) - count(DISTINCT event_id) FROM analytics.web_events
        """)
        log.info("  reenvios (mismo event_id repetido): %d", cur.fetchone()[0])

        cur.execute("""
            SELECT count(*) FROM analytics.web_events WHERE event_time > received_at
        """)
        log.info("  con el reloj adelantado: %d", cur.fetchone()[0])

        cur.execute("""
            SELECT count(*) FROM analytics.web_events WHERE customer_id IS NULL
        """)
        anonimos = cur.fetchone()[0]
        log.info("  anonimos (dato correcto, no suciedad): %d", anonimos)

        # El cambio de hora, medido. Una misma hora LOCAL que corresponde a dos
        # horas UTC distintas no contiene duplicados: son instantes diferentes.
        # Agrupar o deduplicar por hora local se comeria la mitad, sin error.
        #
        # `AT TIME ZONE 'UTC'` explicito en los dos lados: date_trunc sobre un
        # timestamptz usa el huso de la SESION, asi que sin fijarlo el resultado
        # de esta consulta dependeria de quien la lance.
        cur.execute("""
            SELECT to_char(event_time AT TIME ZONE 'Europe/Madrid', 'YYYY-MM-DD HH24') AS hora_local,
                   count(DISTINCT date_trunc('hour', event_time AT TIME ZONE 'UTC')) AS horas_utc,
                   count(*) AS eventos
            FROM analytics.web_events
            GROUP BY 1
            HAVING count(DISTINCT date_trunc('hour', event_time AT TIME ZONE 'UTC')) > 1
            ORDER BY 1
        """)
        repetidas = cur.fetchall()
        for hora_local, horas_utc, eventos in repetidas:
            log.info(
                "  cambio de hora: la hora local %s existe %d veces en UTC (%d eventos)",
                hora_local,
                horas_utc,
                eventos,
            )
        if not repetidas:
            log.warning("  NO hay eventos en el cambio de hora: la trampa no esta cubierta")


# ------------------------------------------------------------------ main ----


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--mode", choices=["initial", "daily"], required=True)
    p.add_argument("--sessions", type=int, default=40_000, help="sesiones en la carga inicial")
    p.add_argument(
        "--history-days",
        type=int,
        default=365,
        help="profundidad del historico. Con 365 se cubren las DOS transiciones "
        "de horario de verano, que es donde la hora local deja de ser fiable",
    )
    p.add_argument("--dirt-factor", type=float, default=1.0, help="multiplicador de suciedad")
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)
    dirt = EventDirtRates.scaled(args.dirt_factor)

    conn = connect()
    try:
        if args.mode == "initial":
            sesiones, dias = args.sessions, args.history_days
        else:
            # Un dia mas de trafico. El historico no se toca: los eventos
            # antiguos son inmutables por definicion.
            sesiones, dias = max(1, args.sessions // 60), 1

        log.info("generando %d sesiones sobre %d dia(s)", sesiones, dias)
        filas = generar(conn, sesiones, dias, dirt)
        copy_rows(conn, "analytics.web_events", EVENT_COLS, filas)
        resumen(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
