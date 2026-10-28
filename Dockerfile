# syntax=docker/dockerfile:1

# Versions pinned together, checked 2026-10-25:
# - Python 3.14.9 on Debian 13 (trixie).
# - Eclipse Temurin 21.0.12.1 JRE. Spark 4.2.0 runs on Java 17, 21 and 25.
# - pyspark 4.2.0 and delta-spark 4.4.1, pinned in pyproject.toml. Delta 4.4.1
#   is built for Spark 4.2.0 (GitHub release notes; the docs table stops at 4.3).
# - uv 0.10.9, the same version as on the host, so both read the same uv.lock.

FROM python:3.14.9-slim-trixie AS base
COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /bin/uv
ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH
# Containers run as uid 1000 so files in data/ belong to the host user. Spark
# fails at startup when its uid has no /etc/passwd entry, so the user is
# created here and not only set in compose.yaml.
RUN useradd --uid 1000 --create-home app
WORKDIR /app

FROM base AS ingestion
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --locked --only-group ingestion
COPY ingestion/ ingestion/
USER app

FROM base AS processing
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_CONF_DIR=/opt/spark/conf
ENV PATH=$JAVA_HOME/bin:$PATH
COPY --from=eclipse-temurin:21.0.12.1_1-jre-noble $JAVA_HOME $JAVA_HOME
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --locked --only-group processing
# Every Spark session in this image gets Delta. The coordinate's version must
# match delta-spark in pyproject.toml.
COPY <<EOF $SPARK_CONF_DIR/spark-defaults.conf
spark.jars.packages              io.delta:delta-spark_4.2_2.13:4.4.1
spark.jars.ivy                   /opt/spark/ivy
spark.sql.extensions             io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog  org.apache.spark.sql.delta.catalog.DeltaCatalog
EOF
# Spark downloads Delta's jars from Maven when a session starts. Starting one
# here fills the Ivy cache at build time, so containers start without Maven.
RUN mkdir /opt/spark/ivy && chown app:app /opt/spark/ivy
USER app
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate().stop()"
COPY processing/ processing/
COPY reconciliation/ reconciliation/
