# Runtime image: Spark + Gluten/Velox + velox_spark, multi-arch.
#
#   scripts/build_image.sh --tag myorg/velox-spark:1.5.0.1 --push
#
# One tag serves linux/amd64 and linux/arm64; the Docker client pulls whichever
# variant matches the host. Nothing is fetched at runtime -- the wheels, Spark
# and the bundle JAR are all baked in, so this runs air-gapped.
#
# Requires `scripts/build_wheels.sh` to have populated ./dist first.

FROM python:3.11-slim-bookworm

ARG SPARK_UID=185
ARG VELOX_SPARK_VERSION

LABEL org.opencontainers.image.title="velox-spark" \
      org.opencontainers.image.description="Apache Spark with the Gluten/Velox native engine" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# JDK 17 is the only system dependency. procps is here because Spark's launcher
# scripts shell out to `ps`, and its absence produces a genuinely baffling
# startup failure.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless procps tini \
    && rm -rf /var/lib/apt/lists/*

COPY dist/ /tmp/dist/

# No --index-url pinning games needed: pip resolves the platform wheel by tag,
# so the amd64 build gets the x86_64 wheel and the arm64 build gets the aarch64
# one automatically. If a platform wheel is missing for this architecture pip
# silently falls back to the pure-Python wheel -- which is why the next step
# exists.
RUN pip install --no-cache-dir --find-links /tmp/dist \
        "velox-spark${VELOX_SPARK_VERSION:+==${VELOX_SPARK_VERSION}}" \
    && rm -rf /tmp/dist

# Build-time gate. `doctor` exits non-zero unless a JAR is bundled and a
# supported JVM is present, so a build that picked up the pure-Python wheel by
# accident fails here instead of shipping an image that silently runs
# unaccelerated Spark.
RUN velox-spark doctor

# Spark writes scratch data as the running user. Matching the upstream Spark
# image's UID of 185 keeps volume permissions consistent for anyone already
# running official Spark images alongside this one.
RUN useradd -u ${SPARK_UID} -m -s /bin/bash spark \
    && mkdir -p /work \
    && chown spark:spark /work
WORKDIR /work
USER ${SPARK_UID}

# tini reaps the JVM's children. Without an init, a killed Spark driver leaves
# zombies that keep the container alive.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python3"]
