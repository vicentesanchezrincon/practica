# Retomar el proyecto

Punto de guardado del **6 de agosto de 2026**. Lee esto para seguir donde lo
dejaste.

---

## Dónde estás

Las nueve fases están hechas. El proyecto llegó a la versión **1.0.0** y al
ejercicio de hotfix.

| Fase | Estado |
|---|---|
| 0 — Git Flow y protección de ramas | hecha |
| 1 — Entorno local (Docker, generador) | PR #1 |
| 2 — VPC, S3, Data Catalog | PR #2 |
| 3 — RDS, Glue Connection, siembra | PR #3 |
| 4 — Ingesta incremental a Bronze | PR #4 |
| 5 — Silver: limpieza, cuarentena, MERGE | PR #5 |
| 6 — Gold: modelo estrella y SCD2 | PR #6 |
| 7 — Step Functions y puerta de calidad | PR #7 |
| 8 — CI/CD con OIDC | PR #8 |
| 9 — Release 1.0.0 y hotfix 1.0.1 | PR #9, #10, y los del hotfix |

!!! IMPORTANTE
**La infraestructura de dev sigue desplegada**, a propósito, para poder recorrer
la arquitectura en la consola de AWS. Cuesta ~0,05 USD/hora. Cuando acabes:

```bash
make destroy-dev
```

Espera unos minutos tras el último job de Glue antes de destruir, o las ENIs
huérfanas dejan el stack de red en `DELETE_FAILED`.

`Practica-Cicd` **no** se destruye con ese comando, y está bien así: es gratis
(solo IAM) y sin él no hay CI.

---

## Arrancar

```bash
cd ~/workspace/projects/practica

aws sso login                  # o como renueves credenciales
make up                        # Postgres + contenedor Glue

make test                      # 97 tests unitarios
make test-infra                # 65 tests de infraestructura
make lint
```

Ver la infraestructura y jugar con ella:

```bash
make watermarks                # hasta donde llego cada tabla
make quality                   # ultimo informe de calidad de Silver
make history                   # ejecuciones de la maquina de estados
make run                       # pipeline completo, orquestado
make db-creds                  # credenciales del RDS
```

Consultar en Athena: bases `practica_dev_silver` y `practica_dev_gold`.

---

## Lo que quedó pendiente de verificar

**El CI nunca llegó a ejecutarse.** El día que se montó, **GitHub Actions estaba
en caída mayor** (confirmado en githubstatus.com): los workflows se registraron
correctamente pero ningún runner llegó a asignarse, y el job se canceló tras 15
minutos con cero pasos ejecutados.

Cuando Actions vuelva:

```bash
gh run list                          # deberia haber ejecuciones
gh workflow run "Deploy dev" --ref develop
gh run watch
```

Lo que sí está verificado sin depender de Actions:

- el rol OIDC está desplegado y su trust policy comprobada con
  `aws iam get-role --role-name practica-github-dev`;
- el mecanismo anti-skip, provocado a mano (exit 0 saltando tests vs exit 4);
- `cdk synth` sin credenciales, exit 0;
- `Practica-Cicd` fuera de `cdk list` sin `-c cicd=true`.

Los **checks obligatorios** (`lint`, `tests-glue`, `infra`) están activados en
`main` y `develop`. Mientras dure la caída, los PR no podrán fusionarse sin
`--admin`.

---

## Pendientes personales

- **Rotar el token de GitHub** `gho_...` que estuvo expuesto en la URL del
  remote. Nunca llegó a ningún commit (verificado: 0 apariciones en el
  histórico), pero tiene scope `repo` completo.
  https://github.com/settings/applications → Authorized OAuth Apps → GitHub CLI
  → Revoke, y después `gh auth login`.
- **`docs/documentacion.html`** es el documento antiguo, congelado en la Fase 3
  y ya superado por `docs/proyecto.md`. Está en `.gitignore` y se puede borrar
  cuando quieras.
- **Registrar Bronze en el Glue Data Catalog** sigue sin decidirse. Cuatro
  opciones evaluadas en el PR #4; la más limpia es hacer de `src/common/config.py`
  la única fuente de verdad del esquema. Está anotado como deuda consciente en
  el capítulo 8 de la documentación.

---

## Documentación

```bash
make docs                      # genera los dos PDF
```

- `docs/proyecto.md` → `documentacion-practica.pdf` — este proyecto
- `docs/glosario.md` → `glosario.pdf` — conceptos reutilizables

Los PDF están en `.gitignore`: se regeneran.
