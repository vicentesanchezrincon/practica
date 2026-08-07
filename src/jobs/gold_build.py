"""Capa Gold: modelo estrella sobre Iceberg.

    glue_catalog.practica_<env>_silver.*  ──este job──►  practica_<env>_gold.*

Tablas que construye:

    dim_date          calendario, generado
    dim_product       SCD tipo 1: se sobrescribe, no guarda historia
    dim_customer      SCD tipo 2: guarda una version por cada cambio
    fct_order_items   los hechos, a grano de LINEA de pedido
    agg_daily_sales   agregado de negocio por dia y categoria

**El grano de fct_order_items es una linea de pedido**, no un pedido. Es el
grano mas atomico disponible, y desde ahi se puede agregar a cualquier nivel
(por pedido, por dia, por cliente). Al reves no se puede. Por eso NO se guarda
`total_amount` del pedido: repetirlo en cada linea lo duplicaria al sumar. El
total de un pedido se obtiene sumando sus lineas.

Se lanza con:
    aws glue start-job-run --job-name practica-dev-gold-build
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common.conciliacion import conciliar, resumen
from common.config import catalog_database
from common.dimensions import (
    FAR_FUTURE,
    UNKNOWN_KEY,
    as_of_join,
    scd2_changes,
    surrogate_key,
    unknown_member,
)
from common.spark_session import CATALOG, build_session

# Atributos de cliente cuyo cambio abre una version nueva en la dimension.
#
# No todos los cambios merecen historia. Corregir una errata en el nombre no
# cambia el analisis; que un cliente pase de "bronze" a "gold" si, porque
# quieres poder decir "cuanto vendimos a clientes gold en marzo" con el segmento
# que tenian ENTONCES.
SCD2_TRACKED = ["segment", "country_code", "marketing_opt_in"]


def log(msg: str) -> None:
    print(f"[gold] {msg}", flush=True)


def silver(spark: SparkSession, environment: str, table: str) -> DataFrame:
    return spark.table(f"{CATALOG}.{catalog_database('silver', environment)}.{table}")


def gold_name(environment: str, table: str) -> str:
    return f"{CATALOG}.{catalog_database('gold', environment)}.{table}"


# Transformaciones de particion oculta de Iceberg.
#
# Hay que usar estas funciones y no F.expr("months(col)"): una expresion
# generica Spark no la reconoce como transformacion de particion y falla con
# "Invalid partition transformation".
PARTITION_TRANSFORMS = {
    "years": F.years,
    "months": F.months,
    "days": F.days,
    "hours": F.hours,
}


def partition_transform(spec: str) -> Column:
    """Convierte 'months(order_date)' en la transformacion que espera Iceberg."""
    nombre, _, columna = spec.partition("(")
    columna = columna.rstrip(")").strip()
    try:
        return PARTITION_TRANSFORMS[nombre.strip()](columna)
    except KeyError:
        raise ValueError(
            f"Transformacion de particion no soportada: {spec!r}. "
            f"Opciones: {sorted(PARTITION_TRANSFORMS)}"
        ) from None


def replace_table(df: DataFrame, full_name: str, partition: str | None = None) -> None:
    """Reescribe la tabla entera.

    Vale para las dimensiones sin historia y para los agregados: son pequenos y
    derivados, asi que reconstruirlos es mas simple y mas seguro que fusionarlos.
    """
    writer = df.writeTo(full_name).using("iceberg")
    if partition:
        writer = writer.partitionedBy(partition_transform(partition))
    writer.createOrReplace()


# ------------------------------------------------------------------ dim_date ---


def build_dim_date(spark: SparkSession, orders: DataFrame, environment: str) -> DataFrame:
    """Calendario que cubre el rango de pedidos.

    Una dimension de fecha se genera, no se extrae: permite preguntar por
    trimestre o por dia de la semana sin escribir funciones de fecha en cada
    consulta, y da un sitio donde colgar festivos o el calendario fiscal.
    """
    rango = orders.select(
        F.min("order_date").alias("desde"), F.max("order_date").alias("hasta")
    ).collect()[0]

    fechas = spark.sql(
        f"SELECT explode(sequence(DATE '{rango['desde']}', DATE '{rango['hasta']}', "
        f"INTERVAL 1 DAY)) AS date"
    )

    dim = fechas.select(
        # La clave de una dimension de fecha es la fecha en formato AAAAMMDD.
        # Es legible de un vistazo y ordena igual que la fecha.
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )

    full = gold_name(environment, "dim_date")
    replace_table(dim, full)
    log(f"dim_date: {dim.count()} dias, de {rango['desde']} a {rango['hasta']}")
    return spark.table(full)


# --------------------------------------------------------------- dim_product ---


def build_dim_product(spark: SparkSession, environment: str) -> DataFrame:
    """SCD tipo 1: refleja el estado actual y no guarda historia.

    Es una decision, no un descuido. Si el precio de catalogo cambia, no
    queremos reescribir el analisis del pasado con el precio nuevo: el precio al
    que se vendio de verdad ya esta en la linea de pedido, que es donde debe
    estar. La dimension solo aporta nombre y categoria.
    """
    productos = silver(spark, environment, "products").select(
        "product_id", "sku", "name", "category", "unit_price", "is_active"
    )

    dim = productos.select(
        surrogate_key("product_id").alias("product_key"),
        "product_id",
        "sku",
        F.col("name").alias("product_name"),
        "category",
        F.col("unit_price").alias("list_price"),
        "is_active",
    )

    dim = dim.unionByName(unknown_member(dim, "product_key"))

    full = gold_name(environment, "dim_product")
    replace_table(dim, full)
    log(f"dim_product: {dim.count() - 1} productos (+ miembro desconocido)")
    return spark.table(full)


# -------------------------------------------------------------- dim_customer ---


def build_dim_customer(spark: SparkSession, environment: str) -> DataFrame:
    """SCD tipo 2: una fila por cada version del cliente.

    El proceso es incremental por necesidad: Silver solo guarda el estado
    ACTUAL de cada cliente (el MERGE sobrescribe), asi que la historia no se
    puede reconstruir a posteriori. Se construye ejecucion a ejecucion,
    comparando lo que llega con lo que ya hay.

    Consecuencia practica: la historia empieza el dia que empiezas a ejecutar
    esto. No hay forma de inventar versiones anteriores.
    """
    full = gold_name(environment, "dim_customer")
    origen = silver(spark, environment, "customers").select(
        "customer_id",
        "email",
        "first_name",
        "last_name",
        "country_code",
        "city",
        "segment",
        "marketing_opt_in",
        F.col("updated_at").alias("_cambio"),
    )

    existe = spark.catalog.tableExists(full)

    if not existe:
        # Primera carga: todo el mundo entra con una unica version abierta.
        # valid_from es el epoch y no `updated_at`: los pedidos anteriores al
        # ultimo cambio del cliente se quedarian sin version a la que apuntar y
        # acabarian todos en el miembro desconocido.
        dim = _nueva_version(origen, desde=F.lit(datetime(1970, 1, 1, tzinfo=UTC)))
        dim = dim.unionByName(_unknown_customer(dim))
        replace_table(dim, full)
        log(f"dim_customer: carga inicial, {dim.count() - 1} clientes")
        return spark.table(full)

    actual = spark.table(full).filter(F.col("customer_key") != UNKNOWN_KEY)
    a_cerrar, a_abrir = scd2_changes(
        origen,
        actual,
        natural_key="customer_id",
        tracked=SCD2_TRACKED,
        effective_from="_cambio",
    )

    n_cerrar = a_cerrar.count()
    n_abrir = a_abrir.count()

    if n_cerrar:
        # Cerrar la version vigente de los que han cambiado. El MERGE toca solo
        # las filas con is_current, que es como mucho una por cliente.
        a_cerrar.createOrReplaceTempView("cambios_cliente")
        spark.sql(f"""
            MERGE INTO {full} t
            USING cambios_cliente s
            ON t.customer_id = s.customer_id AND t.is_current = true
            WHEN MATCHED THEN UPDATE SET
                t.valid_to = s._cierre,
                t.is_current = false
        """)

    if n_abrir:
        nuevas = _nueva_version(a_abrir, desde=F.col("_cambio"))
        nuevas.writeTo(full).append()

    log(f"dim_customer: {n_abrir} versiones nuevas, {n_cerrar} cerradas")
    return spark.table(full)


def _nueva_version(df: DataFrame, desde) -> DataFrame:
    """Da forma de fila de dimension a un conjunto de clientes."""
    return df.select(
        # La clave subrogada incluye valid_from: dos versiones del mismo cliente
        # son filas distintas y necesitan claves distintas.
        surrogate_key(F.col("customer_id").cast("string"), desde.cast("string")).alias(
            "customer_key"
        ),
        "customer_id",
        "email",
        "first_name",
        "last_name",
        "country_code",
        "city",
        "segment",
        "marketing_opt_in",
        desde.cast("timestamp").alias("valid_from"),
        F.lit(FAR_FUTURE).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
    )


def _unknown_customer(dim: DataFrame) -> DataFrame:
    """El miembro desconocido de clientes, valido para siempre.

    Su ventana de validez tiene que cubrir todo el tiempo, porque un hecho
    huerfano puede ser de cualquier fecha.
    """
    return unknown_member(
        dim,
        "customer_key",
        overrides={
            "valid_from": F.lit(datetime(1970, 1, 1, tzinfo=UTC)),
            "valid_to": F.lit(FAR_FUTURE),
            "is_current": F.lit(True),
            "customer_id": F.lit(UNKNOWN_KEY),
        },
    )


# ----------------------------------------------------------- fct_order_items ---


def build_fct_order_items(
    spark: SparkSession,
    environment: str,
    dim_customer: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """Los hechos, a grano de linea de pedido."""
    lineas = silver(spark, environment, "order_items").select(
        "order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_amount"
    )
    pedidos = silver(spark, environment, "orders").select(
        "order_id", "customer_id", "order_date", "status", "currency"
    )

    # Un LEFT JOIN y no INNER: una linea cuyo pedido esta en cuarentena sigue
    # siendo una venta. Perderla descuadraria los ingresos.
    hechos = lineas.join(pedidos, on="order_id", how="left")

    # Version del cliente vigente el dia del pedido. Aqui es donde el SCD2
    # deja de ser teoria.
    hechos = as_of_join(
        hechos,
        dim_customer,
        natural_key="customer_id",
        fact_date="order_date",
        key_column="customer_key",
    )

    productos = dim_product.select("product_id", "product_key")
    hechos = hechos.join(productos, on="product_id", how="left").withColumn(
        "product_key", F.coalesce(F.col("product_key"), F.lit(UNKNOWN_KEY))
    )

    fct = hechos.select(
        F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("customer_key"),
        F.col("product_key"),
        # Dimensiones degeneradas: identificadores sin dimension propia. Viven
        # en el hecho porque no tienen atributos que colgar de ellos.
        F.col("order_id"),
        F.col("order_item_id"),
        F.col("status").alias("order_status"),
        F.col("currency"),
        F.col("order_date"),
        # Medidas, todas aditivas: sumarlas por cualquier dimension da un
        # numero con sentido.
        F.col("quantity"),
        F.col("unit_price"),
        F.col("line_amount"),
    )

    full = gold_name(environment, "fct_order_items")
    replace_table(fct, full, partition="months(order_date)")

    total = fct.count()
    sin_cliente = fct.filter(F.col("customer_key") == UNKNOWN_KEY).count()
    sin_producto = fct.filter(F.col("product_key") == UNKNOWN_KEY).count()
    log(f"fct_order_items: {total} lineas")
    log(f"  al miembro desconocido: {sin_cliente} sin cliente, {sin_producto} sin producto")
    return spark.table(full)


# ---------------------------------------------------------- agg_daily_sales ---


def agregar_ventas_diarias(fct: DataFrame, dim_product: DataFrame) -> DataFrame:
    """Solo el calculo: entra un DataFrame, sale un DataFrame.

    Esta separado de `build_agg_daily_sales` por un motivo concreto: la version
    1.0.0 salio con el ticket medio dividido entre LINEAS y nadie lo vio, porque
    no habia forma de probar este calculo sin levantar un catalogo Iceberg
    entero. Un calculo que no se puede probar en tres lineas es un calculo que
    se volvera a romper.

    **El ticket medio divide entre PEDIDOS DISTINTOS, no entre lineas.** Dividir
    entre lineas da el importe medio por linea, que es otra cosa y siempre sale
    mas bajo: exactamente en la proporcion pedidos/lineas. Es el error de
    agregacion mas comun al pasar de un hecho de cabecera a uno de linea, y no
    da ningun error: el job termina bien, el esquema es correcto y el numero
    esta mal. Ver tests/unit/test_gold_agg.py.
    """
    return (
        fct.join(dim_product, on="product_key", how="left")
        .groupBy(F.col("order_date").alias("sale_date"), "category")
        .agg(
            F.sum("line_amount").alias("revenue"),
            F.sum("quantity").alias("units"),
            # countDistinct cuesta un shuffle extra, si. Es el precio de que el
            # numero signifique lo que dice significar.
            F.countDistinct("order_id").alias("orders"),
            F.count("*").alias("lines"),
        )
        .withColumn("avg_ticket", F.round(F.col("revenue") / F.col("orders"), 2))
        .withColumn("revenue", F.round(F.col("revenue"), 2))
    )


def build_agg_daily_sales(spark: SparkSession, environment: str, fct: DataFrame) -> DataFrame:
    """Ingresos, unidades y ticket medio por dia y categoria."""
    dim_product = spark.table(gold_name(environment, "dim_product")).select(
        "product_key", "category"
    )

    agg = agregar_ventas_diarias(fct, dim_product)

    full = gold_name(environment, "agg_daily_sales")
    replace_table(agg, full, partition="months(sale_date)")
    log(f"agg_daily_sales: {agg.count()} filas (dia x categoria)")
    return spark.table(full)


# ------------------------------------------------ el embudo y la conciliacion ---


def build_fct_sesion(spark: SparkSession, environment: str, dim_customer: DataFrame) -> DataFrame:
    """Una fila por visita, enganchada a la dimension de cliente CONFORMADA.

    Es el segundo hecho del mart y esta a un grano distinto del primero: una
    visita, no una linea de pedido. Que los dos cuelguen de la MISMA
    `dim_customer` es lo que permite preguntar "¿cuanto compran los clientes
    que llegaron por newsletter?" sin duplicar la dimension ni mantener dos
    definiciones de cliente que acabarian divergiendo.

    Las visitas anonimas —dos tercios del trafico— caen en el miembro
    desconocido. Con un INNER JOIN desaparecerian del embudo y la tasa de
    conversion saldria disparatada, porque el denominador seria solo el trafico
    identificado.
    """
    sesiones = silver(spark, environment, "web_sessions")

    # La clave de la visita se resuelve a la version del cliente VIGENTE ese
    # dia: el mismo as-of join que usa fct_order_items, por el mismo motivo.
    con_cliente = as_of_join(
        sesiones,
        dim_customer,
        natural_key="customer_id",
        fact_date="session_start",
        key_column="customer_key",
    )

    fct = con_cliente.select(
        "session_key",
        F.coalesce(F.col("customer_key"), F.lit(UNKNOWN_KEY)).alias("customer_key"),
        F.to_date("session_start").alias("session_date"),
        "session_start",
        "session_end",
        "duracion_segundos",
        "eventos",
        "productos_vistos",
        "device",
        "utm_source",
        "anonima",
        "anadio_al_carrito",
        "inicio_compra",
        "compro",
        F.round(F.col("importe"), 2).alias("importe"),
    )

    full = gold_name(environment, "fct_sesion")
    replace_table(fct, full, partition="months(session_date)")
    log(f"fct_sesion: {fct.count()} visitas")
    return spark.table(full)


def build_agg_funnel_diario(spark: SparkSession, environment: str, fct: DataFrame) -> DataFrame:
    """El embudo por dia, canal y dispositivo.

    Las tasas se calculan aqui y no en la herramienta de BI a proposito: una
    tasa de conversion es una division, y una division hecha sobre un agregado
    ya agregado da la media de las medias, que no es la tasa. Dejandola
    calculada al grano correcto, nadie puede equivocarse despues.
    """
    agg = (
        fct.groupBy("session_date", "utm_source", "device")
        .agg(
            F.count("*").alias("visitas"),
            F.sum(F.when(F.col("productos_vistos") > 0, 1).otherwise(0)).alias("con_producto"),
            F.sum(F.when(F.col("anadio_al_carrito"), 1).otherwise(0)).alias("con_carrito"),
            F.sum(F.when(F.col("inicio_compra"), 1).otherwise(0)).alias("con_checkout"),
            F.sum(F.when(F.col("compro"), 1).otherwise(0)).alias("con_compra"),
            F.round(F.sum("importe"), 2).alias("importe"),
            F.round(F.avg("duracion_segundos"), 1).alias("duracion_media"),
        )
        .withColumn("tasa_conversion", F.round(F.col("con_compra") / F.col("visitas"), 4))
        .withColumn(
            "abandono_carrito",
            # De los que llenaron el carrito, cuantos no compraron. Con cero
            # carritos la tasa no existe: un 0 diria "nadie abandona", que es
            # justo lo contrario de "no hay datos".
            F.when(F.col("con_carrito") == 0, F.lit(None).cast("double")).otherwise(
                F.round(1 - F.col("con_compra") / F.col("con_carrito"), 4)
            ),
        )
    )

    full = gold_name(environment, "agg_funnel_diario")
    replace_table(agg, full, partition="months(session_date)")
    log(f"agg_funnel_diario: {agg.count()} filas (dia x canal x dispositivo)")
    return spark.table(full)


def build_fct_liquidacion(spark: SparkSession, environment: str) -> DataFrame:
    """Los movimientos del proveedor de pagos, al grano de la linea del fichero.

    Se conserva `fichero` como atributo del hecho —una dimension degenerada— y
    no como una dimension aparte: identifica el lote de origen y es lo primero
    que se pregunta cuando un dia no cuadra, pero no tiene atributos propios
    que merezcan una tabla.
    """
    liq = silver(spark, environment, "liquidaciones")

    fct = liq.select(
        "fichero",
        "linea",
        "fecha_liquidacion",
        "fecha_operacion",
        "order_id",
        "tipo",
        "metodo",
        F.round(F.col("importe"), 2).alias("importe"),
        F.round(F.col("comision"), 2).alias("comision"),
        "concepto",
    )

    full = gold_name(environment, "fct_liquidacion")
    replace_table(fct, full, partition="months(fecha_liquidacion)")
    log(f"fct_liquidacion: {fct.count()} movimientos")
    return spark.table(full)


def build_agg_conciliacion(spark: SparkSession, environment: str) -> tuple[DataFrame, dict]:
    """El cuadre diario entre lo que dice Postgres y lo que dice el proveedor.

    Es la unica metrica del mart que cruza dos ORIGENES distintos, y por tanto
    la unica capaz de detectar un fallo que ninguno de los dos ve por separado:
    cada fuente es internamente coherente aunque falte un fichero entero.
    """
    pedidos = silver(spark, environment, "orders").filter(
        F.col("status").isin("paid", "shipped", "delivered", "returned")
    )
    liquidaciones = silver(spark, environment, "liquidaciones")

    agg = conciliar(pedidos, liquidaciones)

    full = gold_name(environment, "agg_conciliacion_diaria")
    replace_table(agg, full, partition="months(dia)")

    cifras = resumen(spark.table(full))
    log(
        f"agg_conciliacion_diaria: {cifras['dias']} dias, "
        f"{cifras['dias_descuadrados']} descuadrados "
        f"(peor: {cifras['peor_descuadre']}, tolerancia {cifras['tolerancia']})"
    )
    return spark.table(full), cifras


# ---------------------------------------------------------------------- main ---


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENVIRONMENT", "BUCKET"])
    environment = args["ENVIRONMENT"]

    spark = build_session(args["JOB_NAME"], warehouse=f"s3://{args['BUCKET']}/gold")
    spark.sparkContext.setLogLevel("WARN")

    log(f"construyendo {catalog_database('gold', environment)}")

    orders = silver(spark, environment, "orders")
    build_dim_date(spark, orders, environment)
    dim_product = build_dim_product(spark, environment)
    dim_customer = build_dim_customer(spark, environment)
    fct = build_fct_order_items(spark, environment, dim_customer, dim_product)
    build_agg_daily_sales(spark, environment, fct)

    # El segundo hecho, a otro grano y colgando de la MISMA dim_customer. Eso
    # es lo que la convierte en una dimension conformada y no en dos tablas
    # que se llaman igual.
    sesiones = build_fct_sesion(spark, environment, dim_customer)
    build_agg_funnel_diario(spark, environment, sesiones)

    build_fct_liquidacion(spark, environment)
    _, conciliacion = build_agg_conciliacion(spark, environment)

    # Cuadre: los ingresos de Gold tienen que coincidir con la suma de las
    # lineas de Silver. Si no cuadran, algun join ha perdido o duplicado filas,
    # y es el tipo de fallo que nadie detecta hasta que el negocio se queja.
    silver_total = (
        silver(spark, environment, "order_items")
        .agg(F.round(F.sum("line_amount"), 2))
        .collect()[0][0]
    )
    gold_total = fct.agg(F.round(F.sum("line_amount"), 2)).collect()[0][0]

    log("--- cuadre ---")
    log(f"  silver.order_items: {silver_total}")
    log(f"  fct_order_items:    {gold_total}")

    if silver_total != gold_total:
        spark.stop()
        raise RuntimeError(
            f"Los ingresos no cuadran: Silver {silver_total} vs Gold {gold_total}. "
            f"Algun join ha perdido o duplicado lineas."
        )

    log("cuadre OK")

    # Cuadre con el TERCERO. El de arriba compara Gold con Silver, es decir el
    # pipeline consigo mismo: detecta joins que pierden o multiplican filas,
    # pero no sabria decir que falta un fichero, porque lo que falta no esta en
    # ninguno de los dos lados.
    #
    # Este compara lo que dice Postgres con lo que dice el proveedor de pagos.
    # Es la unica comprobacion del proyecto que puede cazar un fichero que no
    # llego, uno procesado dos veces, o una referencia cruzada sin normalizar:
    # cada fuente por separado sigue siendo perfectamente coherente.
    log("--- conciliacion con el proveedor de pagos ---")
    log(f"  importe de pedidos:  {conciliacion['importe_pedidos']}")
    log(f"  importe liquidado:   {conciliacion['importe_liquidado']}")
    log(f"  comisiones:          {conciliacion['comisiones']}")
    log(
        f"  dias descuadrados:   {conciliacion['dias_descuadrados']}/{conciliacion['dias']} "
        f"(peor {conciliacion['peor_descuadre']}, tolerancia {conciliacion['tolerancia']})"
    )

    # No se lanza excepcion: un descuadre NO invalida el mart. Los pedidos y las
    # visitas son correctos aunque falte una liquidacion, y tumbar el job
    # dejaria sin datos a quien no tiene nada que ver con el problema.
    #
    # Se publica como senal para que la maquina de estados avise, que es el
    # mismo criterio que en Silver: el job informa, el orquestador decide.
    escribir_senal_de_conciliacion(args["BUCKET"], conciliacion)

    spark.stop()


CONCILIACION_KEY = "_quality/gold/conciliacion.json"


def escribir_senal_de_conciliacion(bucket: str, cifras: dict) -> None:
    """Deja el resultado de la conciliacion en S3, junto al de Silver.

    Mismo patron que `_quality/silver/latest.json`: un JSON pequeno que Step
    Functions puede leer con `CallAwsService` y evaluar con un `Choice`, sin
    tener que parsear logs.
    """
    cuerpo = {
        **cifras,
        "generated_at": datetime.now(UTC).isoformat(),
        "passed": cifras["dias_descuadrados"] == 0,
    }
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=CONCILIACION_KEY,
        Body=json.dumps(cuerpo, indent=2).encode(),
        ContentType="application/json",
    )
    log(f"  senal publicada en s3://{bucket}/{CONCILIACION_KEY}")


if __name__ == "__main__":
    main()
