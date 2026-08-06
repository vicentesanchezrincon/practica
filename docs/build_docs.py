#!/usr/bin/env python3
"""Genera los PDF de la documentacion a partir de los Markdown.

    docs/proyecto.md  ──┐
                        ├─► python-markdown ─► HTML ─► Chrome ─► PDF
    docs/glosario.md  ──┘         + docs/estilo.css

Por que Markdown y no HTML escrito a mano, que es como estaba antes:

  * los dos documentos se leen y se revisan en GitHub sin generar nada,
  * los diff de un PR son legibles: en HTML, cambiar un parrafo mueve etiquetas
    y el diff no dice nada,
  * el indice lo genera la extension `toc` a partir de los encabezados. El que
    habia escrito a mano llevaba tiempo desincronizado del cuerpo, y es el tipo
    de error que nadie detecta porque nadie lee un indice entero.

Por que Chrome y no weasyprint o pandoc: es lo que ya estaba instalado en la
maquina, y es con lo que se genero el PDF original (se ve en los metadatos:
HeadlessChrome + Skia/PDF). Una dependencia menos que instalar y explicar.

Uso:
    make docs                 # los dos documentos
    python3 docs/build_docs.py proyecto
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import markdown

DOCS = Path(__file__).resolve().parent

DOCUMENTOS = {
    # nombre en linea de comandos: (fuente .md, PDF de salida)
    "proyecto": ("proyecto.md", "documentacion-practica.pdf"),
    "glosario": ("glosario.md", "glosario.pdf"),
}

EXTENSIONES = [
    "tables",
    "fenced_code",
    "attr_list",  # {: .ascii } y demas clases sobre un bloque
    "admonition",  # !!! nota / !!! aviso / !!! clave
    "def_list",  # "termino \n : definicion", natural para el glosario
    "toc",  # genera el indice a partir de los encabezados
    "sane_lists",
    "md_in_html",  # permite Markdown dentro de <div class="portada">
]

CONFIG_EXTENSIONES = {
    # Solo dos niveles en el indice: con tres, en el glosario ocuparia mas que
    # el contenido.
    "toc": {"toc_depth": "1-2", "permalink": False},
}

PLANTILLA = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>{titulo}</title>
<link rel="stylesheet" href="{css}">
</head>
<body>
{cuerpo}
</body>
</html>
"""

# Los que estan instalados en el sistema, en orden de preferencia.
NAVEGADORES = ("google-chrome", "chromium", "chromium-browser", "google-chrome-stable")


def navegador() -> str:
    for nombre in NAVEGADORES:
        ruta = shutil.which(nombre)
        if ruta:
            return ruta
    raise SystemExit(
        "No encuentro Chrome ni Chromium, que es lo que convierte el HTML en PDF.\n"
        "Instala uno de: " + ", ".join(NAVEGADORES)
    )


def titulo_de(fuente: Path) -> str:
    """El primer encabezado de nivel 1 del Markdown, para el <title>."""
    for linea in fuente.read_text(encoding="utf-8").splitlines():
        if linea.startswith("# "):
            return linea[2:].strip()
    return fuente.stem


def construir(nombre: str) -> Path:
    md_nombre, pdf_nombre = DOCUMENTOS[nombre]
    fuente = DOCS / md_nombre
    if not fuente.exists():
        raise SystemExit(f"No existe {fuente}")

    conversor = markdown.Markdown(extensions=EXTENSIONES, extension_configs=CONFIG_EXTENSIONES)
    cuerpo = conversor.convert(fuente.read_text(encoding="utf-8"))

    # El HTML intermedio se queda al lado del PDF (esta en .gitignore). Sirve
    # para depurar el estilo sin esperar a la generacion del PDF: se abre en el
    # navegador y con Ctrl+P se ve la paginacion real.
    html = DOCS / f"{fuente.stem}.html"
    html.write_text(
        PLANTILLA.format(titulo=titulo_de(fuente), css="estilo.css", cuerpo=cuerpo),
        encoding="utf-8",
    )

    pdf = DOCS / pdf_nombre
    # Chrome necesita un perfil de usuario escribible; si no se le da uno propio
    # y ya tienes una ventana abierta, se niega a arrancar en headless.
    with tempfile.TemporaryDirectory(prefix="practica-chrome-") as perfil:
        subprocess.run(
            [
                navegador(),
                "--headless=new",
                "--disable-gpu",
                f"--user-data-dir={perfil}",
                # Sin esto, Chrome estampa la fecha, el titulo y la URL
                # file:///home/... en cada pagina.
                "--no-pdf-header-footer",
                f"--print-to-pdf={pdf}",
                html.as_uri(),
            ],
            check=True,
            capture_output=True,
        )

    print(f"{fuente.name:16} -> {pdf.name}  ({pdf.stat().st_size // 1024} KB)")
    return pdf


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "documentos",
        nargs="*",
        choices=[*DOCUMENTOS, []],
        help="cuales generar (por defecto, todos)",
    )
    args = parser.parse_args()

    for nombre in args.documentos or list(DOCUMENTOS):
        construir(nombre)
    return 0


if __name__ == "__main__":
    sys.exit(main())
