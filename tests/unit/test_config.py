"""Tests de la configuracion declarativa.

Son puro Python: no necesitan Spark ni AWS, asi que corren en milisegundos
y sirven de red de seguridad cuando anadas tablas nuevas a TABLES.
"""

from __future__ import annotations

import pytest

from common.config import (
    INGESTION_ORDER,
    QUARANTINE_THRESHOLD,
    TABLES,
    Layout,
    catalog_database,
    get_table,
)


def test_todas_las_tablas_estan_en_el_orden_de_ingesta():
    assert set(INGESTION_ORDER) == set(TABLES)


def test_los_padres_se_ingestan_antes_que_los_hijos():
    """Si order_items se ingesta antes que orders, la validacion de integridad
    referencial de Silver no tiene contra que comparar."""
    posicion = {t: i for i, t in enumerate(INGESTION_ORDER)}
    for spec in TABLES.values():
        for columna, (tabla_padre, _) in spec.references.items():
            assert posicion[tabla_padre] < posicion[spec.name], (
                f"{spec.name}.{columna} referencia a {tabla_padre}, "
                f"que debe ingestarse antes en INGESTION_ORDER"
            )


def test_las_referencias_apuntan_a_tablas_declaradas():
    for spec in TABLES.values():
        for tabla_padre, columna_padre in spec.references.values():
            assert tabla_padre in TABLES
            assert columna_padre in TABLES[tabla_padre].business_key


def test_la_clave_de_negocio_nunca_esta_vacia():
    for spec in TABLES.values():
        assert spec.business_key, f"{spec.name} no tiene clave de negocio"


def test_la_clave_de_negocio_esta_entre_los_not_null():
    """Una clave de negocio con NULL rompe el MERGE de Silver en silencio."""
    for spec in TABLES.values():
        assert set(spec.business_key) <= set(spec.not_null)


@pytest.mark.parametrize("nombre", sorted(TABLES))
def test_la_condicion_de_merge_usa_la_clave_de_negocio(nombre):
    spec = get_table(nombre)
    condicion = spec.merge_condition()
    for clave in spec.business_key:
        assert f"t.{clave} = s.{clave}" in condicion


def test_get_table_falla_con_mensaje_util():
    with pytest.raises(KeyError, match="no declarada"):
        get_table("tabla_que_no_existe")


def test_el_layout_construye_rutas_s3_coherentes():
    layout = Layout(bucket="practica-datalake-123456789012-eu-west-1")
    assert layout.bronze_table("orders").endswith("/bronze/ecommerce/orders")
    assert layout.quarantine.startswith(layout.silver)


def test_el_nombre_de_la_base_del_catalogo_separa_entornos():
    assert catalog_database("silver", "dev") != catalog_database("silver", "prod")
    assert catalog_database("silver", "dev") == "practica_dev_silver"


def test_el_umbral_de_cuarentena_es_un_porcentaje():
    assert 0 < QUARANTINE_THRESHOLD < 1
