"""Configuracion declarativa del pipeline.

Un solo sitio define, por tabla: de donde se saca, cual es su clave de negocio
y que reglas de calidad tiene que cumplir. Los tres jobs (bronze, silver, gold)
leen de aqui, asi que anadir una tabla al pipeline es anadir una entrada a
TABLES, no tocar codigo de Spark.

Hay dos cosas distintas en juego y conviene no mezclarlas:

  * **Como se OBTIENE** una tabla  -> `SourceSpec` y sus subclases.
    Es especifico del tipo de origen: una watermark y una columna de particion
    son conceptos de una lectura JDBC y no significan nada en un fichero.
  * **Como se VALIDA** una tabla   -> el resto de `TableSpec`.
    Es igual para cualquier origen: un email mal escrito lo esta viniera de
    donde viniera.

Hasta la Fase 9 esas dos cosas vivian juntas en `TableSpec`, y no se notaba
porque todos los origenes eran la misma tabla Postgres con `updated_at`. Con un
segundo tipo de origen la costura salta a la vista.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import ClassVar

SOURCE_SYSTEM = "postgres_ecommerce"
SOURCE_SCHEMA = "ecommerce"

# Prefijo de las columnas de linaje que anade Bronze. Se excluyen de las
# comparaciones de negocio (dedup, MERGE) porque cambian en cada ejecucion.
LINEAGE_PREFIX = "_"


# ------------------------------------------------------------------ origen ---


@dataclass(frozen=True)
class SourceSpec:
    """De donde sale una tabla. Una subclase por tipo de origen.

    Se modela con subclases y no con un campo `kind` mas un monton de opciones
    opcionales porque asi **los estados invalidos no se pueden ni escribir**:
    con un unico dataclass seria posible declarar `kind="jdbc"` sin opciones de
    JDBC, y eso solo reventaria en ejecucion.

    `kind` es un ClassVar (no un campo) para que siga siendo un string
    greppable con el que despachar en los jobs, sin poder contradecir a la clase.
    """

    kind: ClassVar[str] = "?"

    system: str = SOURCE_SYSTEM
    """Identificador del sistema de origen. Va a la columna de linaje
    `_source_system`, que es lo que responde "¿de donde salio esta fila?"
    cuando en el lake hay datos de varios sitios."""

    @property
    def bronze_namespace(self) -> str:
        """Carpeta bajo `bronze/` que agrupa las tablas de este origen.

        En JDBC es el esquema de la base de datos. Manteniendo el nombre del
        origen en la ruta, dos tablas que se llamen igual en sistemas distintos
        no se pisan.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class JdbcSource(SourceSpec):
    """Tabla leida por JDBC, de forma incremental por watermark."""

    kind: ClassVar[str] = "jdbc"

    schema: str = SOURCE_SCHEMA

    watermark_column: str = "updated_at"
    """Columna que usa la extraccion incremental. Debe tener indice en origen.

    En una tabla de entidades es `updated_at`. En un flujo de eventos esa
    columna no existe, y hay que elegir entre el momento en que el hecho
    OCURRIO y el momento en que LLEGO. Ver `lookback`."""

    partition_column: str | None = None
    """Columna para paralelizar la lectura JDBC. Sin esto, Spark lee la tabla
    entera con un solo hilo y el job tarda una eternidad.

    Suele ser la PK numerica, pero no tiene por que: Spark tambien trocea por
    fechas y timestamps, que es lo unico posible cuando la clave es un UUID."""

    lookback: timedelta = timedelta(0)
    """Cuanto se retrocede el watermark en cada ejecucion.

    Existe por los datos que llegan tarde. Si la watermark es la hora de
    llegada, no hace falta: lo que llega tarde llega igual, solo que despues.
    Si es la hora del hecho, un evento de ayer que aparece hoy queda **por
    detras de la marca y no se lee nunca**, sin ningun error.

    El precio es releer un solapamiento en cada pasada, y por tanto duplicados
    en Bronze. Es un precio pequeno: Bronze es append-only a proposito y Silver
    deduplica. Perder filas si seria caro."""

    @property
    def bronze_namespace(self) -> str:
        return self.schema


@dataclass(frozen=True)
class FileSource(SourceSpec):
    """Ficheros depositados en una zona de aterrizaje.

    Ninguno de los campos de `JdbcSource` significa nada aqui, y esa es
    exactamente la razon de que el origen sea una jerarquia: no hay watermark
    —lo que marca el avance es el registro de ficheros ya procesados— ni
    columna de particion, porque no hay una consulta que trocear.

    Lo que si hay son cosas que en JDBC no existen, empezando por la
    codificacion: la base de datos entrega texto ya decodificado y un fichero
    entrega bytes.
    """

    kind: ClassVar[str] = "fichero"

    namespace: str = "ficheros"
    landing_prefix: str = ""
    """Carpeta bajo el bucket donde el proveedor deja los ficheros."""

    encoding: str = "utf-8"
    """Y no se adivina. Leer latin-1 como UTF-8 **no lanza ninguna excepcion**
    con la configuracion por defecto: sustituye los bytes que no entiende y
    sigue. El acento se convierte en un simbolo raro y llega hasta Gold."""

    date_format: str = "%d/%m/%Y"
    """El formato de fecha del proveedor, no el tuyo.

    Interpretar dd/mm/aaaa como MM/dd/yyyy no falla ningun dia del mes menor o
    igual que 12: el error parece intermitente y se descarta como "cosa rara"."""

    decimal_comma: bool = False
    """Si los importes usan coma decimal. `to_double("125,40")` en Spark
    devuelve NULL sin avisar, y una columna entera de importes se va a cero."""

    @property
    def bronze_namespace(self) -> str:
        return self.namespace


# ------------------------------------------------------------------ reglas ---


@dataclass(frozen=True)
class TimeSanity:
    """Dos columnas de tiempo que tienen que guardar un orden entre si.

    Sirve para lo que ninguna regla de una sola columna puede ver: un valor
    plausible por si mismo pero imposible en relacion con otro. Un `created_at`
    posterior a su `updated_at` no esta fuera de rango ni es nulo ni incumple
    ningun patron: simplemente no puede haber pasado.
    """

    column: str
    not_after: str
    """Nombre de la otra columna. `column` no puede ser posterior a esta."""

    tolerance: timedelta = timedelta(0)
    """Holgura admitida. Dos relojes nunca van exactamente iguales, y sin margen
    acabas mandando a cuarentena filas correctas."""

    @property
    def reason(self) -> str:
        return f"{self.column}_posterior_a_{self.not_after}"


@dataclass(frozen=True)
class TableSpec:
    """Como se ingesta y se valida una tabla del origen."""

    name: str
    source: SourceSpec
    business_key: list[str]
    """Clave real de negocio. Es la que usa el MERGE de Silver, no la PK tecnica."""

    dedup_order: list[str] = field(default_factory=list)
    """Columnas que deciden que fila sobrevive al dedup, de mas a menos
    prioritaria.

    Se declara en vez de deducirse porque **la respuesta obvia solo existe en
    JDBC**: alli es la watermark, gana la fila modificada mas recientemente. En
    un origen append-only no hay ninguna columna que signifique "esta version es
    posterior a aquella", y elegirla mal borra datos buenos sin dar ni un
    error."""

    dedup_keep: str = "ultima"
    """Cual de las versiones empatadas sobrevive: "ultima" o "primera".

    No es una preferencia estetica, son dos situaciones distintas:

      * una fila **actualizada** varias veces entre dos ingestas -> la ultima
        version es la buena, las anteriores estan obsoletas;
      * un evento **reenviado** por un reintento -> el hecho ocurrio una sola
        vez, y la primera llegada es la que dice cuando llego de verdad.
        Quedarse con la ultima infla la latencia medida sin que nada falle."""

    # --- normalizacion (Silver) ---
    # Se aplica ANTES de validar, para no mandar a cuarentena una fila cuyo
    # unico problema era un espacio sobrante.

    lower_trim: list[str] = field(default_factory=list)
    """Columnas de texto a normalizar con trim + lower. Emails, sobre todo."""

    upper_trim: list[str] = field(default_factory=list)
    """trim + upper. Codigos de pais, divisas, SKUs."""

    # --- validacion (Silver) ---

    not_null: list[str] = field(default_factory=list)

    non_negative: list[str] = field(default_factory=list)
    """Caso particular de `ranges` con minimo 0. Se mantiene aparte por ser con
    diferencia el mas frecuente, y porque `non_negative=["quantity"]` se lee
    mejor que `ranges={"quantity": (0, None)}`."""

    ranges: dict[str, tuple[float | None, float | None]] = field(default_factory=dict)
    """{columna: (minimo, maximo)}, ambos inclusive, cualquiera puede ser None.

    Hace falta cuando `non_negative` no vale, y no vale mas veces de las que
    parece: una devolucion es un importe negativo **correcto**. La regla que
    protege una tabla es el bug de la de al lado."""

    allowed_values: dict[str, list[str]] = field(default_factory=dict)
    """{columna: valores admitidos}. Enumerados: estados, divisas, segmentos.

    Un valor nuevo aqui rara vez es un dato sucio. Casi siempre es el origen
    avisando de que ha cambiado sin decirselo a nadie, que es la forma mas
    barata de enterarse de una migracion ajena."""

    patterns: dict[str, str] = field(default_factory=dict)
    """{columna: regex}. Para lo que la normalizacion no puede arreglar: un
    'ESP' donde se esperaba 'ES' no es un problema de formato, es un dato malo."""

    time_sanity: list[TimeSanity] = field(default_factory=list)
    """Coherencia entre columnas de tiempo. Ver `TimeSanity`."""

    references: dict[str, tuple[str, str]] = field(default_factory=dict)
    """{columna_local: (tabla_padre, columna_padre)} para la integridad referencial."""

    # --- escritura (Silver) ---

    quarantine_threshold: float = 0.05
    """Tasa de cuarentena tolerable para ESTA tabla, entre 0 y 1.

    Es por tabla y no global porque no significan lo mismo: un 3% de clientes
    con el email mal escrito es ruido normal de un formulario web; un 3% de
    pedidos con importes negativos es un incidente.

    Los valores actuales estan calibrados contra la suciedad que inyecta
    data_generator/seed.py. En un proyecto real saldrian de la linea base
    historica de cada tabla, no de una constante escrita a mano."""

    silver_partition: str | None = None
    """Transformacion de particion oculta de Iceberg, p. ej. months(order_date).

    "Oculta" significa que quien consulta filtra por `order_date` y Iceberg
    resuelve solo que particiones leer. En Hive tendrias que filtrar por la
    columna de particion a mano, y todo el mundo se olvida."""

    @property
    def bronze_path_suffix(self) -> str:
        return f"{self.source.bronze_namespace}/{self.name}"

    @property
    def silver_table(self) -> str:
        return self.name

    def merge_condition(self, target: str = "t", source: str = "s") -> str:
        """Condicion ON del MERGE INTO de Iceberg."""
        return " AND ".join(f"{target}.{k} = {source}.{k}" for k in self.business_key)


EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
ISO_COUNTRY_PATTERN = r"^[A-Z]{2}$"

# Vocabularios del negocio. Viven aqui y no en el generador porque los consumen
# los dos: `data_generator/seed.py` los importa para producir datos, y Silver
# los usa para validarlos. Con dos listas separadas, anadir un estado nuevo al
# generador mandaria a cuarentena datos perfectamente buenos.
CATEGORIES = ["electronica", "hogar", "moda", "deporte", "libros", "juguetes", "belleza"]
SEGMENTS = ["bronze", "silver", "gold", "platinum"]
ORDER_STATUS = ["pending", "paid", "shipped", "delivered", "cancelled", "returned"]
COUNTRIES = ["ES", "PT", "FR", "IT", "DE", "NL"]
CURRENCIES = ["EUR"]

EVENT_TYPES = [
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "purchase",
]
DEVICES = ["movil", "escritorio", "tablet"]

# Tipos de movimiento de un fichero de liquidacion.
LIQUIDACION_TIPOS = ["PAGO", "DEVOL", "AJUSTE"]

ANALYTICS_SCHEMA = "analytics"

# Ventana de reproceso de los eventos. Cubre el peor retraso que produce el
# generador (30 horas) con margen: un movil sin cobertura durante un fin de
# semana entero sigue entrando.
EVENT_LOOKBACK = timedelta(hours=48)

# Holgura de las comprobaciones temporales. Postgres escribe created_at y
# updated_at en el mismo INSERT, pero no en el mismo instante.
CLOCK_TOLERANCE = timedelta(seconds=1)

# La de los eventos es mucho mayor, y no por ser menos exigentes: el reloj lo
# pone el movil del visitante. Unos minutos de desviacion son normales; unas
# horas ya no, y eso es lo que se quiere cazar.
CLIENT_CLOCK_TOLERANCE = timedelta(minutes=5)

TABLES: dict[str, TableSpec] = {
    "customers": TableSpec(
        name="customers",
        source=JdbcSource(partition_column="customer_id"),
        business_key=["customer_id"],
        dedup_order=["updated_at"],
        lower_trim=["email"],
        upper_trim=["country_code"],
        not_null=["customer_id", "email"],
        patterns={"email": EMAIL_PATTERN, "country_code": ISO_COUNTRY_PATTERN},
        # country_code se queda con el patron y sin lista de valores a proposito:
        # los paises invalidos que produce el origen ('ESP') ya incumplen el
        # patron, y anadir la lista solo duplicaria el motivo en la misma fila.
        allowed_values={"segment": SEGMENTS},
        time_sanity=[TimeSanity("created_at", "updated_at", CLOCK_TOLERANCE)],
        # Emails y paises mal tecleados: ruido esperable de un formulario.
        quarantine_threshold=0.06,
    ),
    "products": TableSpec(
        name="products",
        source=JdbcSource(partition_column="product_id"),
        business_key=["product_id"],
        dedup_order=["updated_at"],
        upper_trim=["sku"],
        lower_trim=["category"],
        not_null=["product_id", "sku"],
        non_negative=["unit_price"],
        allowed_values={"category": CATEGORIES},
        time_sanity=[TimeSanity("created_at", "updated_at", CLOCK_TOLERANCE)],
        # El catalogo lo mantiene gente, no un formulario publico: se espera limpio.
        quarantine_threshold=0.03,
    ),
    "orders": TableSpec(
        name="orders",
        source=JdbcSource(partition_column="order_id"),
        business_key=["order_id"],
        dedup_order=["updated_at"],
        lower_trim=["status"],
        upper_trim=["currency"],
        not_null=["order_id", "customer_id", "order_date"],
        non_negative=["total_amount"],
        allowed_values={"status": ORDER_STATUS, "currency": CURRENCIES},
        time_sanity=[TimeSanity("created_at", "updated_at", CLOCK_TOLERANCE)],
        references={"customer_id": ("customers", "customer_id")},
        # Un pedido mal formado es dinero que no cuadra: menos tolerancia.
        quarantine_threshold=0.05,
        # Los pedidos se consultan casi siempre por rango de fechas.
        silver_partition="months(order_date)",
    ),
    "order_items": TableSpec(
        name="order_items",
        source=JdbcSource(partition_column="order_item_id"),
        business_key=["order_item_id"],
        dedup_order=["updated_at"],
        not_null=["order_item_id", "order_id", "product_id"],
        non_negative=["quantity", "unit_price", "line_amount"],
        # Mil unidades de la misma linea no es un pedido: es un error de tecleo
        # o una prueba de carga que se colo en produccion. El minimo lo cubre
        # ya `non_negative`, asi que aqui solo hace falta el techo.
        ranges={"quantity": (None, 1000)},
        time_sanity=[TimeSanity("created_at", "updated_at", CLOCK_TOLERANCE)],
        references={
            "order_id": ("orders", "order_id"),
            "product_id": ("products", "product_id"),
        },
        quarantine_threshold=0.05,
    ),
    # ------------------------------------------------ la serie temporal ---
    # Esta entrada es la que justifica todo el refactor de la Fase 10. Compara
    # sus campos con los de arriba: casi ninguna suposicion se mantiene.
    "web_events": TableSpec(
        name="web_events",
        source=JdbcSource(
            schema=ANALYTICS_SCHEMA,
            system="webshop_events",
            # No hay `updated_at`: una fila nunca se modifica. De los dos
            # tiempos que trae el evento se elige el de LLEGADA, no el del
            # hecho. Con `event_time` como marca, un evento de ayer que aparece
            # hoy nace ya por detras del watermark y no se lee jamas.
            watermark_column="received_at",
            # La clave es un UUID: no hay rango numerico que trocear. Spark
            # tambien sabe paralelizar por timestamp, y aqui es la unica opcion.
            partition_column="received_at",
            # Aun leyendo por hora de llegada hace falta solapamiento: un lote
            # puede escribirse en el origen mientras el job esta leyendo.
            lookback=EVENT_LOOKBACK,
        ),
        business_key=["event_id"],
        dedup_order=["received_at"],
        # Un reenvio no es una version mas nueva del hecho: es el mismo hecho
        # contado dos veces. Gana la primera llegada.
        dedup_keep="primera",
        lower_trim=["event_type", "device", "utm_source"],
        # `customer_id` NO esta aqui, y es la diferencia mas importante con
        # `orders`. La mayoria del trafico de una tienda es anonimo: exigirle
        # cliente mandaria a cuarentena dos tercios de los datos buenos, y la
        # tasa de cuarentena dejaria de significar nada.
        not_null=["event_id", "event_time", "received_at", "session_id", "event_type"],
        non_negative=["amount"],
        ranges={"quantity": (None, 1000)},
        allowed_values={"event_type": EVENT_TYPES, "device": DEVICES},
        # Un evento no puede haber ocurrido despues de haber llegado. Con
        # tolerancia amplia porque el reloj lo pone el movil del visitante, no
        # el servidor, y desviarse unos minutos es de lo mas normal.
        time_sanity=[TimeSanity("event_time", "received_at", CLIENT_CLOCK_TOLERANCE)],
        references={
            "customer_id": ("customers", "customer_id"),
            "product_id": ("products", "product_id"),
        },
        # Medido contra los datos reales del generador: 4,00% exacto, con el
        # umbral en 4%. Paso por los pelos, y eso es suerte y no calibracion.
        # 6% deja margen para la variacion normal entre lotes sin dejar de
        # cazar una degradacion de verdad. Un umbral que se cumple por un
        # margen de cero es un umbral que fallara el martes que viene sin que
        # haya cambiado nada.
        quarantine_threshold=0.06,
        # Por dias y no por meses: son ordenes de magnitud mas filas que
        # `orders`, y practicamente toda consulta acota un rango de fechas.
        silver_partition="days(event_time)",
    ),
    # --------------------------------------------- los ficheros del PSP ---
    # Segundo tipo de origen, y el que de verdad pone a prueba `SourceSpec`:
    # aqui no hay watermark, ni columna de particion, ni consulta que trocear.
    "liquidaciones": TableSpec(
        name="liquidaciones",
        source=FileSource(
            system="psp_acme",
            namespace="psp",
            landing_prefix="landing/liquidaciones",
            encoding="latin-1",
            date_format="%d/%m/%Y",
            decimal_comma=True,
        ),
        # El fichero no trae identificador de linea, asi que la clave se compone
        # del fichero y el numero de linea dentro de el. Es lo unico estable:
        # el mismo pedido puede liquidarse dos veces (un pago y su devolucion).
        business_key=["fichero", "linea"],
        dedup_order=["_ingested_at"],
        upper_trim=["tipo", "divisa", "metodo"],
        not_null=["fichero", "linea", "fecha_operacion", "importe"],
        # NO va `non_negative` sobre el importe, y es lo importante de esta
        # tabla: una devolucion es un importe negativo CORRECTO. La regla que
        # protege `orders` mandaria a cuarentena los datos buenos.
        # El techo sigue teniendo sentido: un cobro de un millon es un error.
        ranges={"importe": (-1_000_000.0, 1_000_000.0)},
        allowed_values={"tipo": LIQUIDACION_TIPOS, "divisa": CURRENCIES},
        # `order_id` NO esta en not_null: hay cargos del proveedor que no
        # corresponden a ningun pedido (una penalizacion, un ajuste mensual).
        references={"order_id": ("orders", "order_id")},
        quarantine_threshold=0.03,
        silver_partition="months(fecha_liquidacion)",
    ),
}

INGESTION_ORDER = [
    "customers",
    "products",
    "orders",
    "order_items",
    "web_events",
    "liquidaciones",
]
"""Orden de ingesta. Los padres antes que los hijos, para que la validacion
de integridad referencial de Silver tenga contra que comparar."""


# --------------------------------------------------------------- calidad ---

QUARANTINE_THRESHOLD = 0.05
"""Umbral por defecto, para tablas que no declaren el suyo.

Cada tabla puede afinarlo con `quarantine_threshold`. Si alguna lo supera, el
pipeline se detiene y Gold no se construye sobre un lote sospechoso."""


# ------------------------------------------------------------- ubicaciones ---


@dataclass(frozen=True)
class Layout:
    """Rutas del data lake. El bucket cambia por entorno; el layout no."""

    bucket: str

    @property
    def bronze(self) -> str:
        return f"s3://{self.bucket}/bronze"

    @property
    def silver(self) -> str:
        return f"s3://{self.bucket}/silver"

    @property
    def gold(self) -> str:
        return f"s3://{self.bucket}/gold"

    @property
    def quarantine(self) -> str:
        return f"s3://{self.bucket}/silver/_quarantine"

    def bronze_table(self, table: str) -> str:
        return f"{self.bronze}/{get_table(table).bronze_path_suffix}"


def catalog_database(layer: str, environment: str = "dev") -> str:
    """Nombre de la base en el Glue Data Catalog. Ej: practica_dev_silver."""
    return f"practica_{environment}_{layer}"


def tablas_de(kind: str) -> list[str]:
    """Las tablas de `INGESTION_ORDER` cuyo origen es de ese tipo.

    Existe porque no todo lo que esta en el registro se obtiene igual, y varias
    herramientas solo saben tratar con uno de los tipos: la siembra del RDS y
    el exportador a Parquet hablan JDBC y no tienen nada que hacer con un
    fichero, que llega por su cuenta a la zona de aterrizaje.

    Se pregunta por el TIPO y no se mantiene una lista de excepciones, para que
    anadir una fuente nueva no obligue a acordarse de cada sitio que la
    excluye. La primera version no hacia esto y dos jobs distintos reventaron
    con el mismo AttributeError en la misma ejecucion.
    """
    return [t for t in INGESTION_ORDER if TABLES[t].source.kind == kind]


def get_table(name: str) -> TableSpec:
    try:
        return TABLES[name]
    except KeyError:
        raise KeyError(
            f"Tabla '{name}' no declarada en TABLES. Opciones: {sorted(TABLES)}"
        ) from None
