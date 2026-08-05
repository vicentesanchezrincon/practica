"""Configuracion declarativa del pipeline.

Un solo sitio define, por tabla: la clave de negocio, la columna de watermark
y las reglas de calidad. Los tres jobs (bronze, silver, gold) leen de aqui,
asi que anadir una tabla nueva al pipeline es anadir una entrada a TABLES,
no tocar codigo de Spark.
"""

from __future__ import annotations

from dataclasses import dataclass, field

SOURCE_SYSTEM = "postgres_ecommerce"
SOURCE_SCHEMA = "ecommerce"

# Prefijo de las columnas de linaje que anade Bronze. Se excluyen de las
# comparaciones de negocio (dedup, MERGE) porque cambian en cada ejecucion.
LINEAGE_PREFIX = "_"


@dataclass(frozen=True)
class TableSpec:
    """Como se ingesta y se valida una tabla del origen."""

    name: str
    business_key: list[str]
    """Clave real de negocio. Es la que usa el MERGE de Silver, no la PK tecnica."""

    watermark_column: str = "updated_at"
    """Columna que usa la extraccion incremental. Debe tener indice en origen."""

    partition_column: str | None = None
    """Columna numerica para paralelizar la lectura JDBC. Sin esto, Spark lee
    la tabla entera con un solo hilo y el job tarda una eternidad."""

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

    patterns: dict[str, str] = field(default_factory=dict)
    """{columna: regex}. Para lo que la normalizacion no puede arreglar: un
    'ESP' donde se esperaba 'ES' no es un problema de formato, es un dato malo."""

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
        return f"{SOURCE_SCHEMA}/{self.name}"

    @property
    def silver_table(self) -> str:
        return self.name

    def merge_condition(self, target: str = "t", source: str = "s") -> str:
        """Condicion ON del MERGE INTO de Iceberg."""
        return " AND ".join(f"{target}.{k} = {source}.{k}" for k in self.business_key)


EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
ISO_COUNTRY_PATTERN = r"^[A-Z]{2}$"

TABLES: dict[str, TableSpec] = {
    "customers": TableSpec(
        name="customers",
        business_key=["customer_id"],
        partition_column="customer_id",
        lower_trim=["email"],
        upper_trim=["country_code"],
        not_null=["customer_id", "email"],
        patterns={"email": EMAIL_PATTERN, "country_code": ISO_COUNTRY_PATTERN},
        # Emails y paises mal tecleados: ruido esperable de un formulario.
        quarantine_threshold=0.06,
    ),
    "products": TableSpec(
        name="products",
        business_key=["product_id"],
        partition_column="product_id",
        upper_trim=["sku"],
        lower_trim=["category"],
        not_null=["product_id", "sku"],
        non_negative=["unit_price"],
        # El catalogo lo mantiene gente, no un formulario publico: se espera limpio.
        quarantine_threshold=0.03,
    ),
    "orders": TableSpec(
        name="orders",
        business_key=["order_id"],
        partition_column="order_id",
        lower_trim=["status"],
        upper_trim=["currency"],
        not_null=["order_id", "customer_id", "order_date"],
        non_negative=["total_amount"],
        references={"customer_id": ("customers", "customer_id")},
        # Un pedido mal formado es dinero que no cuadra: menos tolerancia.
        quarantine_threshold=0.05,
        # Los pedidos se consultan casi siempre por rango de fechas.
        silver_partition="months(order_date)",
    ),
    "order_items": TableSpec(
        name="order_items",
        business_key=["order_item_id"],
        partition_column="order_item_id",
        not_null=["order_item_id", "order_id", "product_id"],
        non_negative=["quantity", "unit_price", "line_amount"],
        references={
            "order_id": ("orders", "order_id"),
            "product_id": ("products", "product_id"),
        },
        quarantine_threshold=0.05,
    ),
}

INGESTION_ORDER = ["customers", "products", "orders", "order_items"]
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
        return f"{self.bronze}/{SOURCE_SCHEMA}/{table}"


def catalog_database(layer: str, environment: str = "dev") -> str:
    """Nombre de la base en el Glue Data Catalog. Ej: practica_dev_silver."""
    return f"practica_{environment}_{layer}"


def get_table(name: str) -> TableSpec:
    try:
        return TABLES[name]
    except KeyError:
        raise KeyError(
            f"Tabla '{name}' no declarada en TABLES. Opciones: {sorted(TABLES)}"
        ) from None
