# Imagen de desarrollo local = runtime de AWS Glue 5.0 + las cuatro cosas que
# necesitamos para trabajar y que la imagen oficial no trae.
#
# La imagen base ya incluye Spark, PySpark, boto3 y pytest. Solo faltan el
# generador de datos (faker + psycopg) y las herramientas de estilo.
#
# Importante: NO metas aqui dependencias que usen los jobs de Spark en runtime.
# Esas van al wheel que se sube a S3 y se pasa con --extra-py-files, porque en
# AWS esta imagen no existe.

FROM public.ecr.aws/glue/aws-glue-libs:5

USER root

RUN python3 -m pip install --no-cache-dir --root-user-action=ignore \
        "faker>=30.0" \
        "psycopg[binary]>=3.2" \
        "ruff>=0.6" \
    && python3 -m pip cache purge || true

USER hadoop
