# Retomar el proyecto

Punto de guardado del **4 de agosto de 2026**. Lee esto para seguir donde lo dejaste.

---

## Dónde estás

| Fase | Estado | Rama |
|---|---|---|
| 0 — Git Flow y protección de ramas | hecha | — |
| 1 — Entorno local (Docker, generador) | mergeada | PR #1 |
| 2 — VPC, S3, Data Catalog | mergeada | PR #2 |
| 3 — RDS, Glue Connection, siembra | mergeada | PR #3 |
| **4 — Ingesta incremental a Bronze** | **PR abierto sin mergear** | `feature/bronze-ingest` → [PR #4](https://github.com/vicentesanchezrincon/practica/pull/4) |
| 5 — Silver con Iceberg | pendiente | — |
| 6 — Gold, modelo estrella y SCD2 | pendiente | — |
| 7 — Step Functions | pendiente | — |
| 8 — CI/CD con OIDC | pendiente | — |
| 9 — Release y hotfix | pendiente | — |

**Nada desplegado en AWS.** Se destruyó todo al terminar la sesión.

El plan completo del ejercicio está en
`~/.claude/plans/en-este-proyecto-quiero-playful-firefly.md`.

---

## Primeros pasos mañana

```bash
cd ~/workspace/projects/practica

aws login                          # la sesión de AWS caduca
gh pr view 4                       # revisar la Fase 4
gh pr merge 4 --squash --delete-branch

git checkout develop && git pull
git checkout -b feature/silver-iceberg
```

Levantar el entorno local (no cuesta nada):

```bash
make up
make test        # 38 tests, deben pasar todos
```

Desplegar a AWS solo cuando vayas a probar de verdad (~10 min, el RDS es lo lento):

```bash
make deploy-dev
make export-seed && make seed-rds   # repoblar el RDS
make bronze                         # ingesta a Bronze
```

**Al terminar cada sesión: `make destroy-dev`.**

---

## Decisión pendiente de la Fase 4

Las tablas de Bronze **no están registradas en el Glue Data Catalog**.

Declararlas en CDK obliga a repetir el esquema completo de las cuatro tablas,
que ya vive en `data_generator/schema.sql`, y esa duplicación diverge en cuanto
alguien añade una columna. Silver leerá Bronze por ruta, así que el catálogo
solo aporta poder explorar con Athena.

Tres opciones:

1. Declararlas en CDK y asumir la duplicación.
2. Usar un Glue Crawler — cuesta en cada ejecución y adivina el esquema.
3. Dejar Bronze sin catalogar y registrar solo Silver y Gold, que son las capas
   que de verdad se consultan.
4. **Hacer de `src/common/config.py` la única fuente de verdad del esquema** y
   generar desde ahí tanto el `schema.sql` del origen como las tablas del
   catálogo. Más trabajo ahora, pero elimina la duplicación de raíz: añadir una
   columna pasa a ser tocar un solo fichero.

Está anotado en el PR #4.

### Nota sobre Athena e Iceberg en Bronze

Consultar Bronze con Athena **no es complicado**: es Parquet en S3, solo hace
falta que la tabla exista en el catálogo. Lo más limpio es declararla con
**partition projection**, que evita el `MSCK REPAIR TABLE` y acelera las
consultas:

```sql
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.ingestion_date.type' = 'date',
  'projection.ingestion_date.range' = '2026-01-01,NOW',
  'projection.ingestion_date.format' = 'yyyy-MM-dd'
)
```

Funciona directamente con nuestro layout `ingestion_date=YYYY-MM-DD`.

**Bronze no debe ser Iceberg ni Delta.** Esos formatos resuelven `UPDATE`,
`DELETE`, ACID y time travel — problemas que Bronze no tiene, porque es
append-only e inmutable. A cambio pagarías metadatos crecientes y compactación
que mantener.

El único argumento a favor es que una tabla Iceberg **se registra sola en el
catálogo**, lo que resolvería gratis la duplicación de esquema. Es un beneficio
real, pero es resolver un problema de catálogo con una herramienta de
transaccionalidad.

Y Delta queda descartado: el diseño usa Iceberg, y mezclar los dos formatos en
el mismo lake duplica librerías, configuración de Spark y conocimiento sin
ganar nada.

---

## Qué toca en la Fase 5

`feature/silver-iceberg` — es donde está el grueso del aprendizaje de PySpark:

- Leer la partición del día desde Bronze.
- **Deduplicar** con `row_number()` sobre ventana particionada por clave de
  negocio y ordenada por `updated_at DESC`.
- **Normalizar**: `trim`/`lower` en emails, casteo explícito, fechas a UTC,
  códigos de país.
- **Validar** con las reglas de `src/common/config.py` (`not_null`,
  `non_negative`, `references`). Las filas que fallan van a
  `silver/_quarantine/`, no se descartan en silencio.
- **`MERGE INTO`** sobre tabla Iceberg. La prueba de que está bien: ejecutarlo
  dos veces y que el conteo no cambie.

La configuración de Iceberg para el job de Glue ya está resuelta en
`src/common/spark_session.py`, y el test de humo
(`tests/unit/test_spark_session.py`) confirma que `MERGE INTO` funciona en el
contenedor local.

Ojo con una cosa que ya nos mordió: **si el job usa un servicio nuevo de AWS,
hay que añadir su VPC endpoint**. El test
`test_hay_endpoint_para_cada_servicio_que_usan_los_jobs` lo fuerza.

---

## Cosas que aprendimos por las malas

Todas documentadas en el README, sección "Problemas conocidos":

| Síntoma | Causa |
|---|---|
| `permission denied` en docker.sock | `usermod -aG` no afecta a sesiones abiertas → `newgrp docker` |
| El contenedor no puede escribir en el proyecto | uid 10000 del contenedor vs el tuyo → el Makefile pasa tu GID |
| `DELETE_FAILED` al destruir la red | las ENIs de Glue sobreviven al destroy y bloquean la subred |
| Job de Glue con `ConnectTimeoutError` | falta el VPC endpoint de ese servicio |
| Job de Glue colgado hasta timeout | el security group no se permite a sí mismo, o el rol no puede crear ENIs |

---

## Pendientes personales

- **Rotar el token de GitHub** (`gho_...`) que quedó expuesto en la primera
  sesión. Se revoca en <https://github.com/settings/applications> →
  *Authorized OAuth Apps* → GitHub CLI → Revoke, y luego `gh auth login`.
  Nunca llegó a ningún commit, pero tiene scope `repo` completo.
- Decidir si versionar `docs/documentacion.html` y
  `docs/documentacion-practica.pdf`. Ahora mismo están sin versionar.
