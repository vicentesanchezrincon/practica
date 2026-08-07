"""Capa Bronze para ficheros: zona de aterrizaje -> S3.

    s3://<bucket>/landing/liquidaciones/*.txt ──este job──►
        s3://<bucket>/bronze/psp/liquidaciones/ingestion_date=YYYY-MM-DD/
        s3://<bucket>/silver/_quarantine/_ficheros/    (los que no cuadran)

Es el hermano de `bronze_ingest.py` para un origen que no es una tabla, y se
parece menos de lo que uno esperaria. Tres diferencias de fondo:

**1. La unidad de trabajo es el fichero, no la fila.** Un fichero cuyo pie de
control no cuadra va ENTERO a cuarentena. Media liquidacion no es medio dato
bueno: es un total que no cuadra y del que nadie se entera, porque las lineas
que si llegaron estan perfectamente bien formadas.

**2. La idempotencia se apoya en el contenido, no en el nombre.** Un fichero
reexpedido llega con otro nombre y el mismo contenido. Con el nombre como
clave, la liquidacion de ese dia entra dos veces y el descuadre aparece en un
informe financiero semanas despues.

**3. No hay watermark.** No hay ninguna columna que diga "desde aqui": lo que
marca el avance es el registro de ficheros ya procesados.

Se lanza con:
    aws glue start-job-run --job-name practica-dev-bronze-files
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from common.config import catalog_database, get_table
from common.liquidaciones import (
    Fichero,
    FicheroInvalido,
    descuadre,
    hash_contenido,
    huecos_en_la_secuencia,
    parsear,
)
from common.spark_session import CATALOG, build_session

LANDING_PREFIX = "landing/liquidaciones"
CONTROL_TABLE = "ficheros_procesados"


def log(msg: str) -> None:
    print(f"[bronze-files] {msg}", flush=True)


# ------------------------------------------------------------------ control ---


def control_name(environment: str) -> str:
    return f"{CATALOG}.{catalog_database('silver', environment)}.{CONTROL_TABLE}"


def asegurar_control(spark: SparkSession, nombre: str) -> None:
    """Registro de ficheros ya vistos.

    Es una tabla Iceberg y no DynamoDB a proposito: DynamoDB seria mas
    apropiado para un registro de control con muchas escrituras pequenas, pero
    obligaria a un servicio nuevo, un endpoint de VPC mas y un permiso mas.
    A este ritmo —unas decenas de ficheros al dia— una tabla Iceberg sobra, y
    ademas queda consultable desde Athena, que es justo lo que quieres cuando
    alguien pregunta por que falta la liquidacion del martes.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {nombre} (
            nombre           STRING,
            hash_contenido   STRING,
            fecha_liquidacion DATE,
            secuencia        INT,
            detalles         INT,
            importe          DOUBLE,
            resultado        STRING,
            motivo           STRING,
            procesado_en     TIMESTAMP,
            batch_id         STRING
        ) USING iceberg
    """)


def hashes_ya_procesados(spark: SparkSession, control: str) -> set[str]:
    """Solo los ACEPTADOS.

    Un fichero rechazado por descuadre tiene que poder volver a intentarse: el
    proveedor lo reenviara completo, con el mismo nombre y otro contenido. Si
    los rechazos contaran como procesados, el bueno no entraria nunca.
    """
    filas = spark.sql(
        f"SELECT hash_contenido FROM {control} WHERE resultado = 'aceptado'"  # noqa: S608
    ).collect()
    return {f["hash_contenido"] for f in filas}


# ------------------------------------------------------------------ lectura ---


def listar_landing(s3, bucket: str) -> list[tuple[str, bytes]]:
    """Baja los ficheros de la zona de aterrizaje.

    Se leen enteros en el driver y no con Spark. A este tamano —un dia de
    liquidacion son unos cientos de KB— lo que domina es la semantica por
    fichero: validar el pie, calcular el hash del contenido, decidir si ya se
    proceso. Con ficheros de gigabytes habria que darle la vuelta.
    """
    paginador = s3.get_paginator("list_objects_v2")
    ficheros = []
    for pagina in paginador.paginate(Bucket=bucket, Prefix=LANDING_PREFIX):
        for objeto in pagina.get("Contents", []):
            clave = objeto["Key"]
            if clave.endswith("/"):
                continue
            cuerpo = s3.get_object(Bucket=bucket, Key=clave)["Body"].read()
            ficheros.append((clave.rsplit("/", 1)[-1], cuerpo))
    return sorted(ficheros)


# ---------------------------------------------------------------- escritura ---


def filas_de(fichero: Fichero, batch_id: str, ingested_at: datetime) -> list[Row]:
    """Los detalles, aplanados y con linaje.

    Bronze sigue sin limpiar nada: lo que se guarda es lo que venia, ya
    interpretado a tipos pero sin normalizar ni validar por fila. Lo unico que
    se anade es de donde salio.
    """
    return [
        Row(
            fichero=fichero.nombre,
            proveedor=fichero.proveedor,
            fecha_liquidacion=fichero.fecha_liquidacion,
            secuencia=fichero.secuencia,
            linea=i,
            fecha_operacion=d.fecha_operacion,
            order_id=d.order_id,
            tipo=d.tipo,
            importe=d.importe,
            comision=d.comision,
            divisa=d.divisa,
            metodo=d.metodo,
            concepto=d.concepto,
            _ingested_at=ingested_at,
            _source_system=get_table("liquidaciones").source.system,
            _batch_id=batch_id,
            ingestion_date=ingested_at.date().isoformat(),
        )
        for i, d in enumerate(fichero.detalles, start=1)
    ]


def cuarentena_de_lote(s3, bucket: str, nombre: str, contenido: bytes, motivo: str) -> None:
    """El fichero entero, tal cual llego, con el motivo al lado.

    Se guarda el original y no una version parseada: quien lo mire tiene que
    ver exactamente lo que mando el proveedor, byte a byte, para poder
    reclamarselo.
    """
    destino = f"silver/_quarantine/_ficheros/{datetime.now(UTC).date().isoformat()}/{nombre}"
    s3.put_object(Bucket=bucket, Key=destino, Body=contenido)
    s3.put_object(
        Bucket=bucket,
        Key=f"{destino}.motivo.txt",
        Body=motivo.encode("utf-8"),
        ContentType="text/plain",
    )


# ------------------------------------------------------------------- proceso ---


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENVIRONMENT", "BUCKET"])
    bucket = args["BUCKET"]
    environment = args["ENVIRONMENT"]

    spark = build_session(args["JOB_NAME"], warehouse=f"s3://{bucket}/silver")
    spark.sparkContext.setLogLevel("WARN")
    batch_id = args.get("JOB_RUN_ID") or spark.sparkContext.applicationId
    ingested_at = datetime.now(UTC)

    s3 = boto3.client("s3")
    control = control_name(environment)
    asegurar_control(spark, control)
    vistos = hashes_ya_procesados(spark, control)
    log(f"lote {batch_id}; {len(vistos)} ficheros ya aceptados en el registro")

    ficheros = listar_landing(s3, bucket)
    log(f"{len(ficheros)} ficheros en s3://{bucket}/{LANDING_PREFIX}")

    filas: list[Row] = []
    registro: list[Row] = []
    aceptados = rechazados = repetidos = 0
    secuencias: list[int] = []

    for nombre, contenido in ficheros:
        digest = hash_contenido(contenido)

        if digest in vistos:
            # Mismo contenido que uno ya aceptado, se llame como se llame.
            repetidos += 1
            log(f"  = {nombre}: ya procesado (mismo contenido), se ignora")
            continue

        try:
            fichero = parsear(nombre, contenido)
        except FicheroInvalido as e:
            rechazados += 1
            log(f"  x {nombre}: {e}")
            cuarentena_de_lote(s3, bucket, nombre, contenido, str(e))
            registro.append(
                Row(
                    nombre=nombre,
                    hash_contenido=digest,
                    fecha_liquidacion=None,
                    secuencia=None,
                    detalles=0,
                    importe=0.0,
                    resultado="rechazado",
                    motivo=str(e),
                    procesado_en=ingested_at,
                    batch_id=batch_id,
                )
            )
            continue

        motivo = descuadre(fichero)
        if motivo:
            rechazados += 1
            log(f"  x {nombre}: el pie no cuadra ({motivo})")
            cuarentena_de_lote(s3, bucket, nombre, contenido, f"pie_descuadrado: {motivo}")
            resultado, detalle_motivo = "rechazado", f"pie_descuadrado: {motivo}"
        else:
            aceptados += 1
            secuencias.append(fichero.secuencia)
            filas.extend(filas_de(fichero, batch_id, ingested_at))
            vistos.add(digest)
            log(f"  + {nombre}: {len(fichero.detalles)} detalles, {fichero.importe_real} EUR")
            resultado, detalle_motivo = "aceptado", None

        registro.append(
            Row(
                nombre=nombre,
                hash_contenido=digest,
                fecha_liquidacion=fichero.fecha_liquidacion,
                secuencia=fichero.secuencia,
                detalles=len(fichero.detalles),
                importe=fichero.importe_real,
                resultado=resultado,
                motivo=detalle_motivo,
                procesado_en=ingested_at,
                batch_id=batch_id,
            )
        )

    # Huecos: un dia sin liquidar y un dia sin ventas producen lo mismo, ningun
    # fichero. Solo la secuencia los distingue.
    faltan = huecos_en_la_secuencia(secuencias)
    if faltan:
        log(f"  ! faltan los ficheros de secuencia {faltan}: puede haber dias sin liquidar")

    if filas:
        spec = get_table("liquidaciones")
        destino = f"s3://{bucket}/bronze/{spec.bronze_path_suffix}"
        (
            spark.createDataFrame(filas)
            .withColumn("_ingested_at", F.col("_ingested_at").cast("timestamp"))
            .write.mode("append")
            .partitionBy("ingestion_date")
            .parquet(destino)
        )
        log(f"{len(filas)} detalles -> {destino}")

    if registro:
        spark.createDataFrame(registro).writeTo(control).append()

    log("--- resumen ---")
    log(f"  aceptados:  {aceptados}")
    log(f"  rechazados: {rechazados} (en cuarentena de lote)")
    log(f"  repetidos:  {repetidos} (mismo contenido, ya procesado)")
    log(f"  huecos:     {faltan or 'ninguno'}")

    spark.stop()


if __name__ == "__main__":
    main()
