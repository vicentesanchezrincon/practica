"""Tests del fichero de liquidacion: un origen que no es una tabla.

Python puro, sin Spark. Lo que se fija aqui son los cinco fallos que este tipo
de integracion produce **sin dar ningun error**:

  * leer latin-1 como UTF-8,
  * leer dd/mm/aaaa como MM/dd/yyyy,
  * aceptar un fichero que llego a medias,
  * procesar dos veces el mismo contenido con otro nombre,
  * y no cruzar ninguna fila porque la referencia lleva prefijo.
"""

from __future__ import annotations

from datetime import date

import pytest

from common.liquidaciones import (
    CODIFICACION,
    FicheroInvalido,
    datos_del_nombre,
    descuadre,
    fecha,
    hash_contenido,
    huecos_en_la_secuencia,
    importe,
    nombre_de_fichero,
    normalizar_referencia,
    parsear,
)

CABECERA = "C;LIQ;20260315;001;PSP_ACME"


def detalle(orden: int = 481219, imp: str = "125,40", com: str = "2,81", tipo: str = "PAGO") -> str:
    return f"D;14/03/2026;ORD-{orden:09d};{tipo};{imp};{com};EUR;VISA;Compra online"


def fichero(detalles: list[str], n: int | None = None, imp: str = "", com: str = "") -> bytes:
    """Monta un fichero con el pie ya cuadrado, salvo que se pida lo contrario."""
    if n is None:
        n = len(detalles)
    if not imp:
        imp = f"{sum(importe(d.split(';')[4]) for d in detalles):.2f}".replace(".", ",")
    if not com:
        com = f"{sum(importe(d.split(';')[5]) for d in detalles):.2f}".replace(".", ",")
    lineas = [CABECERA, *detalles, f"P;{n:06d};{imp};{com}"]
    return ("\n".join(lineas) + "\n").encode(CODIFICACION)


# ------------------------------------------------------------- conversiones ---


def test_la_coma_decimal_se_interpreta_bien():
    """`float('125,40')` lanza ValueError, pero `to_double` de Spark devuelve
    NULL sin avisar: una columna entera de importes se va a cero."""
    assert importe("125,40") == 125.40
    assert importe("-45,00") == -45.00
    assert importe(" 0,00 ") == 0.0


def test_el_separador_de_miles_no_se_cuela_en_el_numero():
    assert importe("1.234,56") == 1234.56


def test_la_fecha_es_dia_mes_ano():
    """El fallo silencioso mas caro de esta fuente. Leido como MM/dd/yyyy,
    03/04/2026 es el 4 de marzo en vez del 3 de abril, y no da error NINGUN
    dia del mes menor o igual que 12."""
    assert fecha("03/04/2026") == date(2026, 4, 3)
    assert fecha("14/03/2026") == date(2026, 3, 14)


def test_una_fecha_imposible_en_el_otro_formato_falla_en_voz_alta():
    """El dia 25 no existe como mes, asi que con el formato equivocado esto
    reventaria. Es la unica razon por la que un error de configuracion asi
    llega a descubrirse."""
    assert fecha("25/12/2026") == date(2026, 12, 25)


def test_la_referencia_del_proveedor_se_normaliza_a_la_clave_del_pedido():
    """Cruzar 'ORD-000481219' contra order_id no falla: simplemente no casa
    ninguna fila, y sale una conciliacion vacia que parece que no hubo ventas."""
    assert normalizar_referencia("ORD-000481219") == 481219
    assert normalizar_referencia("ORD-000000001") == 1


def test_una_referencia_que_no_es_un_pedido_no_revienta():
    """Hay cargos del proveedor sin pedido detras: una penalizacion, un ajuste
    mensual. Devuelven None y siguen su camino, no tumban el job."""
    assert normalizar_referencia("AJUSTE-MENSUAL") is None
    assert normalizar_referencia("") is None


# ------------------------------------------------------------------ formato ---


def test_se_separan_los_tres_tipos_de_registro():
    f = parsear("LIQ_PSP_20260315_001.txt", fichero([detalle(), detalle(481220)]))
    assert f.proveedor == "PSP_ACME"
    assert f.fecha_liquidacion == date(2026, 3, 15)
    assert len(f.detalles) == 2
    assert f.declarados == 2


def test_los_acentos_sobreviven_a_la_codificacion():
    """Leido como UTF-8 esto no falla: sustituye los bytes y sigue. El acento
    se convierte en un simbolo raro y viaja hasta Gold."""
    linea = "D;14/03/2026;ORD-000000001;DEVOL;-45,00;-1,01;EUR;MC;Reembolso por articulo dañado"
    f = parsear("LIQ_PSP_20260315_001.txt", fichero([linea]))
    assert f.detalles[0].concepto == "Reembolso por articulo dañado"


def test_un_fichero_sin_pie_no_se_acepta():
    """Sin pie no hay forma de saber si esta completo. Aceptarlo seria
    renunciar a la unica comprobacion de integridad que existe."""
    crudo = (CABECERA + "\n" + detalle() + "\n").encode(CODIFICACION)
    with pytest.raises(FicheroInvalido, match="pie"):
        parsear("LIQ_PSP_20260315_001.txt", crudo)


def test_un_fichero_vacio_no_se_acepta():
    with pytest.raises(FicheroInvalido, match="vacio"):
        parsear("LIQ_PSP_20260315_001.txt", b"")


def test_un_detalle_con_campos_de_menos_invalida_el_fichero():
    crudo = (CABECERA + "\nD;14/03/2026;ORD-000000001\nP;000001;1,00;0,10\n").encode(CODIFICACION)
    with pytest.raises(FicheroInvalido, match="mal formado"):
        parsear("LIQ_PSP_20260315_001.txt", crudo)


# ------------------------------------------------------- el pie de control ---


def test_un_fichero_completo_cuadra():
    f = parsear("LIQ_PSP_20260315_001.txt", fichero([detalle(), detalle(481220)]))
    assert descuadre(f) is None


def test_un_fichero_truncado_no_cuadra_aunque_se_lea_perfectamente():
    """Las lineas que llegaron estan bien formadas: sin el pie, nada delata que
    falta media liquidacion."""
    crudo = fichero([detalle()], n=2, imp="250,80", com="5,62")
    f = parsear("LIQ_PSP_20260315_001.txt", crudo)
    assert "detalles_declarados=2 leidos=1" in descuadre(f)


def test_un_fichero_con_todos_los_registros_pero_mal_sumado_tampoco_cuadra():
    """Peor que el truncado: significa que el emisor y tu no estais mirando lo
    mismo. Por eso se comprueban las tres cosas del pie y no solo el recuento."""
    crudo = fichero([detalle()], imp="999,99")
    f = parsear("LIQ_PSP_20260315_001.txt", crudo)
    assert "importe_declarado" in descuadre(f)


def test_una_comision_mal_declarada_se_detecta():
    crudo = fichero([detalle()], com="99,99")
    f = parsear("LIQ_PSP_20260315_001.txt", crudo)
    assert "comision_declarada" in descuadre(f)


def test_las_devoluciones_negativas_cuadran_igual():
    """Un importe negativo es CORRECTO aqui. La regla non_negative que protege
    `orders` mandaria a cuarentena los datos buenos."""
    f = parsear(
        "LIQ_PSP_20260315_001.txt",
        fichero([detalle(), detalle(481220, imp="-45,00", com="-1,01", tipo="DEVOL")]),
    )
    assert descuadre(f) is None
    assert f.importe_real == 80.40


# ------------------------------------------------------------ idempotencia ---


def test_el_mismo_contenido_con_otro_nombre_tiene_el_mismo_hash():
    """La reexpedicion llega con otro nombre. Apoyar la idempotencia en el
    nombre duplica la liquidacion de un dia entero."""
    crudo = fichero([detalle()])
    assert hash_contenido(crudo) == hash_contenido(crudo)
    assert nombre_de_fichero(date(2026, 3, 15), 1) != nombre_de_fichero(
        date(2026, 3, 15), 1, reenvio=True
    )


def test_un_contenido_distinto_tiene_otro_hash():
    """El caso contrario: el proveedor reenvia el fichero ya COMPLETO, con el
    mismo nombre. Ese si tiene que entrar."""
    assert hash_contenido(fichero([detalle()])) != hash_contenido(
        fichero([detalle(), detalle(481220)])
    )


def test_del_nombre_se_saca_la_fecha_y_la_secuencia():
    assert datos_del_nombre("LIQ_PSP_20260315_007.txt") == (date(2026, 3, 15), 7)
    assert datos_del_nombre("LIQ_PSP_20260315_007_R.txt") == (date(2026, 3, 15), 7)


def test_un_nombre_que_no_encaja_se_reconoce_como_tal():
    assert datos_del_nombre("informe_marzo.xlsx") is None


# ------------------------------------------------------------------ huecos ---


def test_se_detecta_el_fichero_que_no_llego():
    """Un dia sin liquidar y un dia sin ventas producen lo mismo: ningun
    fichero. Solo la secuencia distingue "no vendimos" de "falta el dinero de
    un dia"."""
    assert huecos_en_la_secuencia([1, 2, 4, 5]) == [3]
    assert huecos_en_la_secuencia([1, 2, 3]) == []


def test_varios_huecos_seguidos_se_listan_todos():
    assert huecos_en_la_secuencia([10, 14]) == [11, 12, 13]


def test_sin_ficheros_no_hay_huecos_que_declarar():
    """Distinto de "faltan todos": si no ha llegado nada, no hay rango sobre el
    que opinar y decir lo contrario seria una alarma falsa cada domingo."""
    assert huecos_en_la_secuencia([]) == []
