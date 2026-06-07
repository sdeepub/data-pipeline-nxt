# Data Pipeline NxT

Real-time industrial data pipeline built with Apache Flink, Kafka, IoTDB, and Grafana.

## Status

✅ MVP Complete (v0.1.0)

The end-to-end pipeline has been validated and is operational.

```text
Simulator → Kafka → Flink → IoTDB → Grafana
```

### Validation Results

* Kafka ingestion verified
* Flink stream processing verified
* IoTDB persistence verified
* Grafana dashboards verified
* Docker deployment verified
* Smoke tests passing (7/7)

---

## Architecture

```text
┌───────────┐
│ Simulator │
└─────┬─────┘
      │
      ▼
┌───────────┐
│   Kafka   │
└─────┬─────┘
      │
      ▼
┌───────────┐
│   Flink   │
└─────┬─────┘
      │
      ▼
┌───────────┐
│   IoTDB   │
└─────┬─────┘
      │
      ▼
┌───────────┐
│ Grafana   │
└───────────┘
```

---

## Technology Stack

* Java 11
* Apache Kafka
* Apache Flink
* Apache IoTDB
* Grafana
* Docker Compose

---

## Quick Start

### Build Images

```bash
docker compose build --no-cache
```

### Start the Platform

```bash
docker compose up -d
```

### Optional Scaling

Run multiple simulators:

```bash
docker compose up -d --scale simulator=10
```

Run multiple Flink TaskManagers:

```bash
docker compose up -d --scale flink-taskmanager=2
```

---

## Verify Deployment

Run the smoke test:

```bash
./tests/smoke/smoke-test.sh
```

Expected result:

```text
Results: 7 / 7 tests passed
```

---

## Access Services

| Service        | URL                   |
| -------------- | --------------------- |
| Grafana        | http://localhost:3000 |
| Flink UI       | http://localhost:8081 |
| IoTDB REST API | http://localhost:8080 |

Grafana default credentials:

```text
admin / admin
```

---

## Grafana Setup

If dashboards show no data, verify the IoTDB datasource.

Datasource settings:

```text
Type: IoTDB
URL: http://iotdb:8080
```

Save and test the datasource connection.

---

## Example Sensor Event

```json
{
  "machine_id": "MC001",
  "machine_type": "turbine",
  "location": "zone_B",
  "timestamp": 1717769347000,
  "gas_temperature": 72.5,
  "gas_pressure": 45.2,
  "humidity": 55.8,
  "spin_rate": 1200.0,
  "torque": 85.3,
  "status": "running",
  "fault_code": null
}
```

---

## Project Structure

```text
data-pipeline-nxt/
├── docker-compose.yml
├── flink-java/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/iotdb/flink/
│       ├── KafkaToIoTDB.java
│       ├── SensorData.java
│       ├── ddl/
│       ├── dlq/
│       ├── manifest/
│       └── operators/
├── simulators/
├── grafana/
├── tests/
│   └── smoke/
└── README.md
```

---

## Smoke Test Coverage

The smoke test validates:

* Docker services
* Kafka topic availability
* Kafka message flow
* Flink job execution
* IoTDB connectivity
* IoTDB data persistence
* End-to-end pipeline health

Run:

```bash
./tests/smoke/smoke-test.sh
```

---

## Development

Build the Flink application:

```bash
cd flink-java
mvn clean package
```

Generated artifact:

```text
target/kafka-iotdb-pipeline.jar
```

---

## Current MVP Features

* Real-time Kafka ingestion
* Flink stream processing
* IoTDB time-series persistence
* Grafana dashboards
* Docker-based deployment
* Automated smoke testing

---

## Roadmap (v0.2.0)

Production readiness:

* Flink checkpointing
* Flink savepoints
* Kafka consumer lag monitoring
* Pipeline modularization
* CI pipeline automation

Future phases:

* Dead Letter Queue (DLQ)
* Schema validation
* Integration testing
* Prometheus metrics
* OpenTelemetry tracing
* Centralized logging
* KPI aggregation
* Anomaly detection
* Workflow integration
* Kubernetes deployment
* Terraform infrastructure
* Disaster recovery testing

---

## Tested Environment

* Debian 12
* Docker Compose v2
* Java 11

---

## License

TBD
