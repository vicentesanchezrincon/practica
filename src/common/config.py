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

    not_null: list[str] = field(default_factory=list)
    non_negative: list[str] = field(default_factory=list)

    references: dict[str, tuple[str, str]] = field(default_factory=dict)
    """{columna_local: (tabla_padre, columna_padre)} para la integridad referencial."""

    @property
    def bronze_path_suffix(self) -> str:
        return f"{SOURCE_SCHEMA}/{self.name}"

    @property
    def silver_table(self) -> str:
        return self.name

    def merge_condition(self, target: str = "t", source: str = "s") -> str:
        """Condicion ON del MERGE INTO de Iceberg."""
        return " AND ".join(f"{target}.{k} = {source}.{k}" for k in self.business_key)


TABLES: dict[str, TableSpec] = {
    "customers": TableSpec(
        name="customers",
        business_key=["customer_id"],
        partition_column="customer_id",
        not_null=["customer_id", "email"],
        non_negative=[],
    ),
    "products": TableSpec(
        name="products",
        business_key=["product_id"],
        partition_column="product_id",
        not_null=["product_id", "sku"],
        non_negative=["unit_price"],
    ),
    "orders": TableSpec(
        name="orders",
        business_key=["order_id"],
        partition_column="order_id",
        not_null=["order_id", "customer_id", "order_date"],
        non_negative=["total_amount"],
        references={"customer_id": ("customers", "customer_id")},
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
    ),
}

INGESTION_ORDER = ["customers", "products", "orders", "order_items"]
"""Orden de ingesta. Los padres antes que los hijos, para que la validacion
de integridad referencial de Silver tenga contra que comparar."""


# --------------------------------------------------------------- calidad ---

QUARANTINE_THRESHOLD = 0.05
"""Si mas del 5% de las filas de un lote acaban en cuarentena, el pipeline se
para y no construye Gold. Mejor no publicar nada que publicar datos malos."""


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
