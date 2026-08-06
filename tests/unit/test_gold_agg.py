"""Tests del agregado de negocio de Gold.

Este fichero existe por un incidente real. La version 1.0.0 salio con el ticket
medio de `agg_daily_sales` calculado dividiendo los ingresos entre las LINEAS de
pedido en lugar de entre los pedidos distintos.

Lo importante de ese incidente no es el bug, es como pudo llegar a produccion:

  * el job termino SUCCEEDED,
  * el esquema de la tabla era correcto, asi que nada de forma salto,
  * el cuadre de ingresos paso, porque comprueba `revenue`, no `avg_ticket`,
  * la puerta de calidad paso, porque mide Silver y no Gold,
  * y **no habia ningun test sobre gold_build.py**. La suite entera siguio en
    verde con el bug dentro.

Se detecto consultando Athena y cuadrando a mano: si `avg_ticket` es el ticket
medio por pedido, `avg_ticket * orders` tiene que aproximar `revenue`. Con el
bug aproximaba siempre menos, exactamente en la proporcion pedidos/lineas.

Los datos de aqui estan elegidos para que **las dos respuestas posibles no
puedan confundirse**: 2 pedidos, 4 lineas, 400 de ingresos. Lo correcto es
200,00 y el bug daba 100,00. Un test con un pedido de una sola linea habria
pasado con las dos versiones, que es la trampa de este tipo de bug.
"""

from __future__ import annotations

from datetime import date

import pytest

pytest.importorskip("pyspark", reason="Ejecuta los tests dentro del contenedor Glue")

from jobs.gold_build import agregar_ventas_diarias  # noqa: E402

DIA = date(2026, 3, 1)


@pytest.fixture
def dim_product(spark):
    return spark.createDataFrame(
        [(101, "hogar"), (102, "hogar")],
        "product_key long, category string",
    )


def hechos(spark, filas):
    """filas: (order_id, product_key, quantity, line_amount)"""
    return spark.createDataFrame(
        [(o, p, DIA, q, float(a)) for o, p, q, a in filas],
        "order_id long, product_key long, order_date date, quantity int, line_amount double",
    )


def una_fila(agg):
    filas = agg.collect()
    assert len(filas) == 1, f"se esperaba una sola fila agregada, hay {len(filas)}"
    return filas[0]


# ------------------------------------------------------------- el incidente ---


def test_el_ticket_medio_divide_entre_pedidos_distintos(spark, dim_product):
    """2 pedidos, 4 lineas, 400 de ingresos -> 200,00 por pedido."""
    fct = hechos(
        spark,
        [
            (1, 101, 1, 100.0),
            (1, 102, 1, 100.0),
            (1, 101, 1, 100.0),
            (2, 102, 1, 100.0),
        ],
    )
    assert una_fila(agregar_ventas_diarias(fct, dim_product))["avg_ticket"] == 200.00


def test_el_ticket_medio_no_es_el_importe_medio_por_linea(spark, dim_product):
    """El bug de la 1.0.0, fijado por su valor concreto.

    Con los mismos datos, dividir entre lineas daba 100,00. Este test es
    redundante con el anterior y esta a proposito: nombra el error, de modo que
    si alguien vuelve a "optimizar" el countDistinct, el informe de fallo diga
    exactamente que se rompio.
    """
    fct = hechos(
        spark,
        [
            (1, 101, 1, 100.0),
            (1, 102, 1, 100.0),
            (1, 101, 1, 100.0),
            (2, 102, 1, 100.0),
        ],
    )
    assert una_fila(agregar_ventas_diarias(fct, dim_product))["avg_ticket"] != 100.00


def test_las_lineas_de_un_pedido_no_inflan_el_conteo_de_pedidos(spark, dim_product):
    fila = una_fila(
        agregar_ventas_diarias(
            hechos(spark, [(1, 101, 1, 50.0), (1, 102, 2, 50.0), (2, 101, 1, 50.0)]),
            dim_product,
        )
    )
    assert fila["orders"] == 2
    assert fila["lines"] == 3


# ------------------------------------------------------------- agregaciones ---


def test_los_ingresos_y_las_unidades_son_la_suma_de_las_lineas(spark, dim_product):
    fila = una_fila(
        agregar_ventas_diarias(
            hechos(spark, [(1, 101, 2, 30.0), (1, 102, 3, 20.5), (2, 101, 1, 49.5)]),
            dim_product,
        )
    )
    assert fila["revenue"] == 100.00
    assert fila["units"] == 6


def test_el_avg_ticket_se_puede_reconstruir_desde_revenue_y_orders(spark, dim_product):
    """El cuadre que delato el bug en Athena, aqui como test.

    Es la propiedad que define la medida: si no se cumple, `avg_ticket` no es un
    ticket medio por pedido aunque se llame asi.
    """
    fila = una_fila(
        agregar_ventas_diarias(
            hechos(spark, [(1, 101, 1, 120.0), (1, 102, 1, 80.0), (2, 101, 1, 200.0)]),
            dim_product,
        )
    )
    assert round(fila["avg_ticket"] * fila["orders"], 2) == fila["revenue"]


def test_se_agrupa_por_dia_y_categoria(spark):
    """Dos categorias distintas no se mezclan en una fila."""
    dim = spark.createDataFrame(
        [(101, "hogar"), (102, "deporte")], "product_key long, category string"
    )
    agg = agregar_ventas_diarias(hechos(spark, [(1, 101, 1, 10.0), (1, 102, 1, 90.0)]), dim)
    por_categoria = {f["category"]: f for f in agg.collect()}
    assert set(por_categoria) == {"hogar", "deporte"}
    assert por_categoria["deporte"]["revenue"] == 90.00


def test_un_hecho_sin_categoria_no_se_pierde(spark, dim_product):
    """El join es LEFT: un producto que no este en la dimension sigue contando.

    Con un INNER JOIN, esos ingresos desaparecerian del agregado sin que nada
    fallara, que es el mismo tipo de error que resuelve el miembro desconocido.
    """
    fct = hechos(spark, [(1, 999, 1, 75.0)])
    fila = una_fila(agregar_ventas_diarias(fct, dim_product))
    assert fila["category"] is None
    assert fila["revenue"] == 75.00
