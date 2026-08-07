# Retomar el proyecto

Punto de guardado del **6 de agosto de 2026**. Lee esto para seguir donde lo
dejaste.

---

## Dónde estás

**7 de agosto de 2026.** Las nueve fases originales y las cuatro de la
**ampliación** están cerradas. El pipeline tiene ahora **tres orígenes de
naturaleza distinta**, no uno repetido.

| Fase | Rama | Estado |
|---|---|---|
| 10 | `feature/source-spec` | Fusionada (PR #14), CI verde |
| 11 | `feature/timescale-events` | Fusionada (PR #15), CI verde |
| 12 | `feature/landing-files` | Fusionada (PR #16), CI verde |
| 13 | `feature/gold-conciliacion` | PR #17 abierto |

**El CI ya funciona.** La caída mayor de GitHub Actions que bloqueó la sesión
anterior se resolvió; los tres PR de la ampliación se fusionaron con los tres
jobs en verde y sin `--admin`. La ejecución confirmó lo que solo se había
provocado a mano: `tests-glue` corre **209 tests con 0 saltados** dentro de la
imagen de Glue.

La infraestructura de dev **sigue desplegada**. Se destruye con `make
destroy-dev`, esperando unos minutos tras el último job de Glue para evitar ENIs
huérfanas.

### Lo que hay que terminar

**Bronze mezcla dos generaciones de datos.** Durante la verificación se
re-sembró el RDS, y `seed-rds` hace `TRUNCATE ... RESTART IDENTITY`: los ids se
reutilizan para filas distintas, así que el histórico de Bronze pasó a describir
una base de datos que ya no existe. El síntoma es que `orders` sube al 7,69% de
cuarentena y la conciliación reporta días con más pedidos que movimientos.

No es un defecto del código: es el estado en que quedó el lake. Se arregla con

```bash
make reset-bronze      # pide confirmación explícita; borra s3://.../bronze/
make bronze
make silver FULL=1
make gold
```

En producción esto no se hace, porque en producción no se trunca el origen.

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
