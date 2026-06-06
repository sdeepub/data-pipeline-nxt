# ─── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /build

# Copy POM first (cached layer if source doesn't change)
COPY pom.xml .

# Download dependencies safely
RUN echo "[Builder] Downloading Maven dependencies..." && \
    mvn dependency:go-offline -q

# Copy source code
COPY src/ src/

# Build FAT JAR with shade plugin
RUN echo "[Builder] Compiling and packaging Java project..." && \
    mvn clean package && \
    echo "[Builder] Build complete. JAR files created:" && \
    ls -lh target/*.jar && \
    echo "[Builder] Verifying kafka-iotdb-pipeline.jar exists..." && \
    test -f target/kafka-iotdb-pipeline.jar || \
    (echo "[Builder] ERROR: kafka-iotdb-pipeline.jar not found!" && exit 1)

# ─── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM flink:1.17.2-scala_2.12-java11

LABEL maintainer="data-pipeline" \
      description="Flink Kafka → IoTDB pipeline (Java)" \
      version="1.0"

# User-submitted jobs belong safely isolated in usrlib/
# All SQL connector dependencies are bundled directly inside our packaged FAT JAR
COPY --from=builder /build/target/kafka-iotdb-pipeline.jar /opt/flink/usrlib/

# Verify JAR exists in runtime image
RUN echo "[Runtime] Verifying JAR in runtime image..." && \
    ls -lh /opt/flink/usrlib/kafka-iotdb-pipeline.jar && \
    echo "[Runtime] JAR verification successful!"

# Set explicit environment fallbacks
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
    KAFKA_TOPIC=sensor-topic \
    KAFKA_GROUP_ID=flink-java-pipeline \
    IOTDB_HOST=iotdb \
    IOTDB_PORT=6667 \
    IOTDB_USER=root \
    IOTDB_PASSWORD=root \
    IOTDB_STORAGE_GROUP=root.factory1

RUN echo "[Runtime] Image ready. Flink version:" && \
    flink --version