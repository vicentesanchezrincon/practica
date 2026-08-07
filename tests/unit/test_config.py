"""Tests de la configuracion declarativa.

Son puro Python: no necesitan Spark ni AWS, asi que corren en milisegundos
y sirven de red de seguridad cuando anadas tablas nuevas a TABLES.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from common.config import (
    INGESTION_ORDER,
    LINEAGE_PREFIX,
    QUARANTINE_THRESHOLD,
    TABLES,
    FileSource,
    JdbcSource,
    Layout,
    SourceSpec,
    catalog_database,
    get_table,
    tablas_de,
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


def test_toda_tabla_declara_como_desempatar_el_dedup():
    """Sin `dedup_order` no hay forma de saber que fila sobrevive cuando una
    clave aparece repetida, y elegir mal descarta datos buenos en silencio."""
    for spec in TABLES.values():
        assert spec.dedup_order, f"{spec.name} no declara dedup_order"


def test_un_origen_jdbc_nunca_desempata_por_el_linaje():
    """Ordenar por `_ingested_at` haria que el superviviente dependiera de
    cuando se ejecuto el job y no de los datos.

    La regla vale **donde el origen ofrece un orden**, que es el caso de JDBC:
    ahi siempre hay una columna que dice que version es posterior. Un origen de
    ficheros no la tiene, y ahi el linaje es la respuesta correcta y no un
    atajo. Ver el test siguiente.
    """
    for spec in TABLES.values():
        if isinstance(spec.source, JdbcSource):
            assert not any(c.startswith(LINEAGE_PREFIX) for c in spec.dedup_order), spec.name


def test_un_origen_de_ficheros_si_puede_desempatar_por_el_linaje():
    """En un fichero no hay ninguna columna que ordene versiones del mismo dato.

    La clave (fichero, linea) es unica dentro de un fichero, y el registro de
    control impide procesar dos veces el mismo contenido. Un duplicado solo
    puede venir de un reproceso deliberado, y entonces la ingesta mas reciente
    ES la buena. Que este test exista es para que la excepcion sea una decision
    y no un descuido que nadie revisa.
    """
    spec = get_table("liquidaciones")
    assert isinstance(spec.source, FileSource)
    assert spec.dedup_order == ["_ingested_at"]


# --------------------------------------------------------------- origenes ---


def test_todo_origen_declara_su_tipo():
    """`kind` es lo que mira cada job para saber como leer la tabla."""
    for spec in TABLES.values():
        assert isinstance(spec.source, SourceSpec)
        assert spec.source.kind != "?", f"{spec.name} usa SourceSpec en crudo"


def test_la_ruta_de_bronze_sale_del_origen_y_no_de_una_constante():
    """Dos sistemas pueden tener una tabla con el mismo nombre. Si la ruta no
    lleva el origen, la segunda pisa a la primera sin avisar."""
    assert get_table("orders").bronze_path_suffix == "ecommerce/orders"


def test_un_origen_generico_no_sabe_donde_escribir():
    """SourceSpec es una base abstracta: quien anada un tipo de origen tiene
    que decidir su espacio de nombres, no heredar uno por accidente."""
    with pytest.raises(NotImplementedError):
        _ = SourceSpec().bronze_namespace


def test_los_enumerados_declarados_no_estan_vacios():
    for spec in TABLES.values():
        for columna, admitidos in spec.allowed_values.items():
            assert admitidos, f"{spec.name}.{columna} declara una lista vacia"


def test_los_rangos_tienen_al_menos_un_extremo():
    """Un rango (None, None) no valida nada y se lee como si validara algo."""
    for spec in TABLES.values():
        for columna, (minimo, maximo) in spec.ranges.items():
            assert minimo is not None or maximo is not None, f"{spec.name}.{columna}"
            if minimo is not None and maximo is not None:
                assert minimo <= maximo


def test_las_comprobaciones_temporales_usan_dos_columnas_distintas():
    for spec in TABLES.values():
        for comprobacion in spec.time_sanity:
            assert comprobacion.column != comprobacion.not_after
            assert comprobacion.tolerance >= timedelta(0)


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


def test_el_exportador_solo_recorre_las_tablas_que_estan_en_postgres():
    """`export_seed.py` recorre INGESTION_ORDER, que ahora tiene tablas que NO
    salen de la base de datos.

    Lo encontro una ejecucion real, no la suite: el exportador pidio el esquema
    de un origen de ficheros y reviento con un AttributeError. Se filtra por el
    TIPO de origen y no por una lista de excepciones, para que una fuente nueva
    no obligue a acordarse de aquel fichero.
    """
    de_postgres = tablas_de("jdbc")
    assert "liquidaciones" not in de_postgres
    assert "web_events" in de_postgres
    for tabla in de_postgres:
        assert get_table(tabla).source.schema


def test_ningun_job_recorre_el_registro_entero_a_ciegas():
    """El fallo que se repitio TRES veces en la misma ejecucion.

    `INGESTION_ORDER` dejo de ser homogeneo en cuanto entro un origen de
    ficheros: `seed_rds`, `export_seed` y `bronze_ingest` lo recorrian entero y
    los tres reventaron con un AttributeError al pedirle a un fichero un
    esquema, o una watermark, o una ventana de reproceso.

    Todos hablan JDBC, asi que todos tienen que declararlo. Este test lee el
    codigo fuente porque el fallo no esta en una funcion que se pueda invocar:
    esta en la linea que elige sobre que iterar.
    """
    raiz = Path(__file__).resolve().parents[2]
    consumidores = [
        raiz / "src" / "jobs" / "bronze_ingest.py",
        raiz / "src" / "jobs" / "seed_rds.py",
        raiz / "data_generator" / "export_seed.py",
    ]
    for fichero in consumidores:
        codigo = fichero.read_text()
        assert 'tablas_de("jdbc")' in codigo, (
            f"{fichero.name} tiene que declarar que solo trata origenes JDBC. "
            f"Recorrer INGESTION_ORDER entero revienta con la primera fuente "
            f"que no sea una tabla."
        )
        # Se busca la ITERACION, no la mencion: en los comentarios y en los
        # textos de ayuda el nombre puede seguir apareciendo, y de hecho debe.
        for patron in ("= INGESTION_ORDER", "in INGESTION_ORDER"):
            assert patron not in codigo, (
                f"{fichero.name} itera sobre INGESTION_ORDER entero, y ya no "
                f"todas sus tablas se obtienen igual."
            )


def test_silver_si_recorre_el_registro_entero_y_esta_bien():
    """El contraste que hace util al test anterior.

    Silver lee de Bronze, y en Bronze todas las tablas son ya lo mismo:
    ficheros Parquet en una ruta. Le da igual como llegaron. Es el unico
    consumidor al que el tipo de origen no le afecta, y por eso es el unico que
    puede recorrer el registro entero.
    """
    codigo = (
        Path(__file__).resolve().parents[2] / "src" / "jobs" / "silver_transform.py"
    ).read_text()
    assert "INGESTION_ORDER" in codigo
