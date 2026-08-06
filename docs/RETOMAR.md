# Retomar el proyecto

Punto de guardado del **5 de agosto de 2026**. Lee esto para seguir donde lo dejaste.

---

## Dónde estás

| Fase | Estado | Rama |
|---|---|---|
| 0 — Git Flow y protección de ramas | hecha | — |
| 1 — Entorno local (Docker, generador) | mergeada | PR #1 |
| 2 — VPC, S3, Data Catalog | mergeada | PR #2 |
| 3 — RDS, Glue Connection, siembra | mergeada | PR #3 |
| 4 — Ingesta incremental a Bronze | mergeada | PR #4 |
| **5 — Silver: limpieza, cuarentena, MERGE Iceberg** | **PR abierto sin mergear** | `feature/silver-iceberg` → [PR #5](https://github.com/vicentesanchezrincon/practica/pull/5) |
| 6 — Gold, modelo estrella y SCD2 | pendiente | — |
| 7 — Step Functions | pendiente | — |
| 8 — CI/CD con OIDC | pendiente | — |
| 9 — Release y hotfix | pendiente | — |

**Nada desplegado en AWS.** Se destruyó todo al terminar la sesión.

El plan completo está en `~/.claude/plans/en-este-proyecto-quiero-playful-firefly.md`.

---

## Primeros pasos mañana

```bash
cd ~/workspace/projects/practica

aws login                          # la sesión de AWS caduca
gh pr view 5                       # revisar la Fase 5
gh pr merge 5 --squash --delete-branch

git checkout develop && git pull
git checkout -b feature/gold-marts
```

Entorno local (no cuesta nada):

```bash
make up
make test        # 80 tests unitarios
make test-infra  # 34 tests de infraestructura
```

Desplegar solo cuando vayas a probar de verdad (~10 min, el RDS es lo lento):

```bash
make deploy-dev
make export-seed && make seed-rds   # repoblar el RDS
make bronze                         # RDS  -> Bronze
make silver                         # Bronze -> Silver
make quality                        # informe de calidad
```

Los watermarks se borran con el `destroy`, así que la primera `make bronze` de
mañana será una carga completa. Si Bronze devuelve 0 filas sin explicación tras
redesplegar, quedaron watermarks viejos: `make reset-watermarks`.

**Al terminar cada sesión: `make destroy-dev`.**

---

## PENDIENTE: glosario de conceptos

Hay términos que han ido apareciendo y que conviene tener explicados en un sitio
fijo, con ejemplos de este mismo proyecto. Crear `docs/GLOSARIO.md` con:

**Modelado dimensional** (lo que toca en la Fase 6)
- **Modelo estrella** — por qué una tabla de hechos rodeada de dimensiones y no
  un esquema normalizado; qué problema resuelve.
- **Tabla de hechos** vs **dimensión** — cómo se decide qué es cada cosa.
- **Grano** de una tabla de hechos — la pregunta "¿qué representa una fila?" y
  por qué equivocarse aquí lo estropea todo.
- **Clave subrogada** vs **clave natural / de negocio**.
- **SCD tipo 1 / tipo 2** — conservar historia de los cambios de una dimensión.
- **Dimensión "desconocido"** — la fila `-1` a la que apuntan los hechos
  huérfanos, y por qué existe.
- **Snowflake** y por qué normalmente no compensa.

**Calidad e integridad**
- **Referencialmente cerrada** — que ninguna fila apunte a algo que no está en
  la misma capa. Silver **no** lo es a propósito: ver la sección de la Fase 5 en
  el README. Explicar el compromiso y qué se gana.
- **Clave foránea huérfana** — y la distinción clave que descubrimos: apuntar a
  algo que *nunca existió* vs a algo que *no sobrevivió a la limpieza*.
- **Efecto cascada / amplificación** de la cuarentena.
- **Cuarentena** vs descartar en silencio.

**Arquitectura y procesos**
- **Medallion** (bronze / silver / gold) — qué responsabilidad tiene cada capa.
- **Idempotencia** — y por qué "el job no dio error" no la demuestra.
- **Watermark** y extracción incremental (CDC por marca de tiempo).
- **Upsert** / `MERGE INTO`.
- **Linaje** — para qué sirven `_batch_id`, `_ingested_at`, `_source_system`.
- **Backfill** / reprocesado.

**Formatos y almacenamiento**
- **Iceberg** vs Parquet plano vs Delta — qué aporta un *table format*.
- **Partición oculta** de Iceberg y por qué mejora a Hive.
- **Time travel** y snapshots.
- **Problema de los ficheros pequeños** y la compactación.
- **Schema evolution**.

---

## Qué toca en la Fase 6

`feature/gold-marts` — modelo estrella sobre Iceberg:

- `dim_customer` como **SCD tipo 2** (`valid_from`, `valid_to`, `is_current`).
  Es el ejercicio más valioso de todo el proyecto para una entrevista.
- `dim_product`, `dim_date`.
- `fct_orders` con claves subrogadas hacia las dimensiones.
- `agg_daily_sales`: ingresos, unidades y ticket medio por día y categoría.
- `fct_orders` particionada con `months(order_date)`.

**Herencia directa de la Fase 5 que hay que resolver aquí:** Silver **no es
referencialmente cerrada**. Puede haber un pedido cuyo `customer_id` esté en
cuarentena y por tanto no exista en `silver.customers`. Gold tendrá que hacer
`LEFT JOIN` y mandar esos hechos a una **dimensión "desconocido"** (la fila
`-1`), que es exactamente lo que se hace en un modelo estrella real. No se
pueden descartar: son ventas reales y el total de ingresos tiene que cuadrar.

---

## Cosas que aprendimos por las malas

Todas documentadas en el README, sección "Problemas conocidos":

| Síntoma | Causa |
|---|---|
| `permission denied` en docker.sock | `usermod -aG` no afecta a sesiones abiertas → `newgrp docker` |
| El contenedor no puede escribir en el proyecto | uid 10000 del contenedor vs el tuyo → el Makefile pasa tu GID |
| `ruff` falla siempre en el mismo fichero | se creó en una shell de `newgrp docker` y quedó con grupo `docker` |
| `DELETE_FAILED` al destruir la red | las ENIs de Glue sobreviven al destroy y bloquean la subred |
| Job de Glue con `ConnectTimeoutError` | falta el VPC endpoint de ese servicio |
| Job de Glue colgado hasta timeout | el SG no se permite a sí mismo, o el rol no puede crear ENIs |
| Bronze devuelve 0 filas tras redesplegar | los watermarks viven en SSM y sobreviven al `destroy` |
| Las tablas no aparecen en Athena | Iceberg escribió con catálogo Hadoop: la detección de Glue fallaba |

---

## Pendientes personales

- **Rotar el token de GitHub** (`gho_...`) expuesto en la primera sesión. Se
  revoca en <https://github.com/settings/applications> → *Authorized OAuth Apps*
  → GitHub CLI → Revoke, y luego `gh auth login`. Nunca llegó a ningún commit,
  pero tiene scope `repo` completo.
- Decidir si versionar `docs/documentacion.html` y
  `docs/documentacion-practica.pdf`. Siguen sin versionar.
- **Decisión abierta de la Fase 4**: las tablas de Bronze no están registradas
  en el Glue Data Catalog. Cuatro opciones evaluadas en el PR #4; la más limpia
  a medio plazo es hacer de `src/common/config.py` la única fuente de verdad del
  esquema y generar desde ahí. Silver y Gold sí se registran solas por ser
  Iceberg, así que esto solo afecta a poder explorar Bronze con Athena.
