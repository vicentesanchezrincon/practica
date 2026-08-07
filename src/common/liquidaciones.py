"""Lectura de ficheros de liquidacion: multi-registro con pie de control.

Un fichero de intercambio regulado no es una tabla. Lleva **tres esquemas
distintos dentro**, distinguidos por el primer campo:

    C;LIQ;20260315;001;PSP_ACME                          <- cabecera
    D;14/03/2026;ORD-000481219;PAGO;125,40;2,81;EUR;VISA;Compra online
    P;000482;123456,78;2765,43                           <- pie de control

Leerlo como un CSV no da error: da columnas desplazadas y filas basura
mezcladas con las buenas. Hay que separar por tipo de registro primero.

El pie es la unica prueba de integridad que existe. Un fichero que llego a
medias se lee perfectamente —las lineas que hay estan bien formadas—, y lo
unico que delata que faltan es que el recuento y las sumas no cuadren. Por eso
aqui la cuarentena es **de lote y no de fila**: media liquidacion no es medio
dato bueno, es un total que no cuadra.

Todo el modulo es Python puro y sin Spark: son ficheros pequenos y lo que
domina es la semantica por fichero (validar el pie, calcular el hash, decidir
si ya se proceso), no el volumen. Con ficheros de gigabytes habria que darle la
vuelta y leer con Spark.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime

CABECERA = "C"
DETALLE = "D"
PIE = "P"
SEPARADOR = ";"

CODIFICACION = "latin-1"
"""Como llegan estos ficheros de verdad.

Leerlos como UTF-8 **no lanza ninguna excepcion** con la configuracion por
defecto: los bytes que no encajan se sustituyen y el proceso sigue. El acento
se convierte en un simbolo raro y viaja hasta Gold sin que nada lo pare.
"""

FORMATO_FECHA = "%d/%m/%Y"
"""dd/mm/aaaa, el formato europeo.

El fallo silencioso mas caro de esta fuente: `03/04/2026` leido como
MM/dd/yyyy es el 4 de marzo en vez del 3 de abril, y **no da error ningun dia
del mes menor o igual que 12**. Solo a partir del dia 13 empieza a fallar, con
lo que un error de configuracion parece intermitente.
"""

PATRON_NOMBRE = re.compile(r"^LIQ_PSP_(?P<fecha>\d{8})_(?P<secuencia>\d{3})(?P<sufijo>_R)?\.txt$")
PREFIJO_REFERENCIA = "ORD-"


def nombre_de_fichero(dia: date, secuencia: int, *, reenvio: bool = False) -> str:
    return f"LIQ_PSP_{dia:%Y%m%d}_{secuencia:03d}{'_R' if reenvio else ''}.txt"


def datos_del_nombre(nombre: str) -> tuple[date, int] | None:
    """(fecha, secuencia) a partir del nombre, o None si no encaja.

    La secuencia importa: sirve para detectar huecos. Un dia que no se liquido
    y un dia sin ventas producen lo mismo —ningun fichero— y solo el numero de
    secuencia los distingue.
    """
    encaje = PATRON_NOMBRE.match(nombre)
    if not encaje:
        return None
    return (
        datetime.strptime(encaje["fecha"], "%Y%m%d").date(),
        int(encaje["secuencia"]),
    )


def importe(texto: str) -> float:
    """Convierte '125,40' o '-45,00' a float.

    La coma decimal no es una curiosidad local: `float("125,40")` lanza
    ValueError, pero `to_double` de Spark devuelve **NULL sin avisar**. Una
    columna entera de importes a nulo, y las sumas dan cero.
    """
    return float(texto.strip().replace(".", "").replace(",", "."))


def fecha(texto: str) -> date:
    return datetime.strptime(texto.strip(), FORMATO_FECHA).date()


def normalizar_referencia(referencia: str) -> int | None:
    """'ORD-000481219' -> 481219.

    La referencia del proveedor no es la clave del pedido: lleva prefijo y
    ceros a la izquierda. Cruzarla tal cual contra `orders.order_id` no falla,
    simplemente **no casa ninguna fila**, y el resultado es una conciliacion
    perfectamente vacia que parece que no hubo ventas.
    """
    texto = referencia.strip().removeprefix(PREFIJO_REFERENCIA).lstrip("0")
    return int(texto) if texto.isdigit() else None


@dataclass(frozen=True)
class Detalle:
    fecha_operacion: date
    order_id: int | None
    tipo: str
    importe: float
    comision: float
    divisa: str
    metodo: str
    concepto: str


@dataclass(frozen=True)
class Fichero:
    """Un fichero ya troceado en sus tres partes."""

    nombre: str
    hash_contenido: str
    proveedor: str
    fecha_liquidacion: date
    secuencia: int
    detalles: list[Detalle]
    declarados: int
    importe_declarado: float
    comision_declarada: float

    @property
    def importe_real(self) -> float:
        return round(sum(d.importe for d in self.detalles), 2)

    @property
    def comision_real(self) -> float:
        return round(sum(d.comision for d in self.detalles), 2)


class FicheroInvalido(Exception):
    """El fichero no se puede interpretar. Va entero a cuarentena."""


def hash_contenido(bytes_: bytes) -> str:
    """Identidad por CONTENIDO, no por nombre.

    Es lo unico que caza un reenvio bajo otro nombre, que es como llegan de
    verdad las reexpediciones. Apoyar la idempotencia en el nombre significa
    duplicar la liquidacion de un dia entero.
    """
    return hashlib.sha256(bytes_).hexdigest()


def parsear(nombre: str, contenido: bytes) -> Fichero:
    """Trocea el fichero. Lanza FicheroInvalido si no se puede interpretar.

    Decodifica en latin-1 con `errors="strict"` a proposito: si el fichero no
    es lo que dice ser, se quiere saber, no arreglarlo por lo bajo.
    """
    try:
        texto = contenido.decode(CODIFICACION)
    except UnicodeDecodeError as e:
        raise FicheroInvalido(f"no se puede decodificar en {CODIFICACION}: {e}") from e

    lineas = [ln for ln in texto.splitlines() if ln.strip()]
    if not lineas:
        raise FicheroInvalido("fichero vacio")

    cabeceras = [ln for ln in lineas if ln.startswith(CABECERA + SEPARADOR)]
    pies = [ln for ln in lineas if ln.startswith(PIE + SEPARADOR)]
    crudos = [ln for ln in lineas if ln.startswith(DETALLE + SEPARADOR)]

    if len(cabeceras) != 1:
        raise FicheroInvalido(f"se esperaba 1 cabecera, hay {len(cabeceras)}")
    if len(pies) != 1:
        # Sin pie no hay forma de saber si el fichero esta completo. Aceptarlo
        # seria renunciar a la unica comprobacion de integridad que existe.
        raise FicheroInvalido(f"se esperaba 1 pie, hay {len(pies)}")

    campos_cabecera = cabeceras[0].split(SEPARADOR)
    campos_pie = pies[0].split(SEPARADOR)
    if len(campos_cabecera) < 5 or len(campos_pie) < 4:
        raise FicheroInvalido("cabecera o pie con campos de menos")

    try:
        detalles = [_detalle(ln) for ln in crudos]
        return Fichero(
            nombre=nombre,
            hash_contenido=hash_contenido(contenido),
            proveedor=campos_cabecera[4].strip(),
            fecha_liquidacion=datetime.strptime(campos_cabecera[2].strip(), "%Y%m%d").date(),
            secuencia=int(campos_cabecera[3]),
            detalles=detalles,
            declarados=int(campos_pie[1]),
            importe_declarado=importe(campos_pie[2]),
            comision_declarada=importe(campos_pie[3]),
        )
    except (ValueError, IndexError) as e:
        raise FicheroInvalido(f"registro mal formado: {e}") from e


def _detalle(linea: str) -> Detalle:
    campos = linea.split(SEPARADOR)
    if len(campos) < 8:
        raise ValueError(f"detalle con {len(campos)} campos, se esperaban 9: {linea[:60]}")
    return Detalle(
        fecha_operacion=fecha(campos[1]),
        order_id=normalizar_referencia(campos[2]),
        tipo=campos[3].strip().upper(),
        importe=importe(campos[4]),
        comision=importe(campos[5]),
        divisa=campos[6].strip().upper(),
        metodo=campos[7].strip().upper(),
        concepto=campos[8].strip() if len(campos) > 8 else "",
    )


# Margen del cuadre del pie. Los importes vienen ya redondeados a dos
# decimales, asi que cualquier diferencia real es de al menos un centimo; el
# margen solo absorbe el error de coma flotante de sumar miles de valores.
TOLERANCIA_CUADRE = 0.005


def descuadre(fichero: Fichero) -> str | None:
    """El motivo por el que el pie no cuadra, o None si cuadra.

    Se comprueban las tres cosas que declara el pie y no solo el recuento: un
    fichero puede traer todos los registros y aun asi no sumar lo que dice, que
    es peor, porque significa que el emisor y tu no estais mirando lo mismo.
    """
    if fichero.declarados != len(fichero.detalles):
        return f"detalles_declarados={fichero.declarados} leidos={len(fichero.detalles)}"
    if abs(fichero.importe_declarado - fichero.importe_real) > TOLERANCIA_CUADRE:
        return f"importe_declarado={fichero.importe_declarado} real={fichero.importe_real}"
    if abs(fichero.comision_declarada - fichero.comision_real) > TOLERANCIA_CUADRE:
        return f"comision_declarada={fichero.comision_declarada} real={fichero.comision_real}"
    return None


def huecos_en_la_secuencia(secuencias: list[int]) -> list[int]:
    """Los numeros que faltan entre el minimo y el maximo recibidos.

    Un dia sin liquidar y un dia sin ventas producen lo mismo: ningun fichero.
    Solo la secuencia los distingue, y es la diferencia entre "no vendimos" y
    "nos falta el dinero de un dia".
    """
    if not secuencias:
        return []
    presentes = set(secuencias)
    return [n for n in range(min(presentes), max(presentes) + 1) if n not in presentes]
