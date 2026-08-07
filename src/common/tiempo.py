"""Zona horaria: lo unico que hay que saber es que la hora local miente.

Regla del proyecto: **todo se guarda en UTC y la hora local es una vista**. En
UTC el tiempo avanza siempre, sin saltos ni repeticiones, y por eso es lo unico
sobre lo que se puede agrupar, deduplicar y comparar sin sorpresas.

Dos veces al ano la hora local de Madrid deja de ser una funcion biyectiva del
instante:

  * el ultimo domingo de octubre el reloj se atrasa y **la hora local
    02:00-02:59 ocurre dos veces**, separadas por una hora real. Dos eventos
    con la misma hora local no son un duplicado, y deduplicar por ella se come
    la mitad sin dar ningun error;
  * el ultimo domingo de marzo el reloj se adelanta y **esa misma hora local no
    existe**. Una marca de tiempo ahi es imposible, y aun asi la conversion
    devuelve algo.

Es el problema numero uno de cualquier serie temporal, y en el sector
energetico llega a ser normativo: los ficheros de medidas horarias declaran un
dia de 23 horas y otro de 25, con una columna extra que dice cual de las dos
lecturas de las 02:00 es.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

MADRID = ZoneInfo("Europe/Madrid")


def transiciones_horarias(desde: date, hasta: date) -> list[tuple[date, str]]:
    """Los dias del intervalo en que cambia la hora, y en que sentido.

    Se detectan comparando el desfase UTC al principio y al final de cada dia,
    en lugar de codificar "el ultimo domingo de marzo". Esa regla **no es una
    constante**: ya ha cambiado en el pasado, no es la misma en todos los
    paises, y hay una directiva europea para suprimirla. Preguntarselo a la
    base de datos de husos es lo unico que no caduca.

    Devuelve pares (dia, "adelanta"|"atrasa").
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


def _instantes_de(local: datetime) -> tuple[datetime, datetime]:
    """Los dos instantes UTC a los que puede corresponder una hora local.

    Python resuelve la ambiguedad con `fold` (PEP 495): 0 y 1 son las dos
    lecturas posibles. En una hora normal las dos dan el mismo instante.
    """
    return (
        local.replace(fold=0).astimezone(UTC),
        local.replace(fold=1).astimezone(UTC),
    )


def hora_ambigua(local: datetime) -> bool:
    """Si esa hora local ocurre DOS veces (el dia que el reloj se atrasa).

    Ojo con la implementacion evidente, que esta mal: comparar los desfases de
    `fold=0` y `fold=1` da True tambien para la hora que **no existe**, porque
    `fold` cubre las dos anomalias. Son problemas opuestos —una duplica y la
    otra borra— y confundirlos lleva a tratarlos igual.

    Lo que si las distingue es el **signo**. En la hora repetida, la segunda
    lectura es posterior; en la que no existe, el orden se invierte.
    """
    primero, segundo = _instantes_de(local)
    return segundo > primero


def hora_inexistente(local: datetime) -> bool:
    """Si esa hora local NO ocurre nunca (el dia que el reloj se adelanta).

    Una marca de tiempo aqui es imposible, y aun asi `astimezone` devuelve algo
    en vez de fallar. Por eso hay que preguntarlo a proposito: nada te va a
    avisar.
    """
    primero, segundo = _instantes_de(local)
    return segundo < primero
