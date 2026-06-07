# Data Pipeline NxT

Real-time industrial data pipeline built with Apache Flink, Kafka, IoTDB, and Grafana.

## Status

✅ MVP Complete

End-to-end pipeline validated:

```text
Simulator → Kafka → Flink → IoTDB → Grafana
```

Smoke Test: **7/7 passed**

## Architecture

```text
Simulator
    ↓
Kafka
    ↓
Flink
    ↓
IoTDB
    ↓
Grafana
```

### Data Flow

* Simulator generates machine telemetry
* Kafka receives JSON events
* Flink processes and enriches events
* IoTDB stores time-series data
* Grafana visualizes real-time metrics

## Stack

* Apache Kafka
* Apache Flink
* Apache IoTDB
* Grafana
* Docker Compose
* Java 11

## Quick Start

### Start

```bash
docker compose up -d
```

### Verify

Run the smoke test:

```bash
./tests/smoke/smoke-test.sh
```

Expected result:

```text
Results: 7 / 7 tests passed
```

### Access

| Service    | URL                   |
| ---------- | --------------------- |
| Grafana    | http://localhost:3000 |
| Flink UI   | http://localhost:8081 |
| IoTDB REST | http://localhost:8080 |
| Kafka      | localhost:9092        |

Grafana default credentials:

```text
admin / admin
```

## Example Sensor Event

```json
{
  "machine_id": "MC001",
  "timestamp": 1717769347000,
  "gas_temperature": 72.5,
  "gas_pressure": 45.2,
  "humidity": 55.8,
  "spin_rate": 1200.0,
  "torque": 85.3
}
```

## Development

Build the Flink job:

```bash
cd flink-java
mvn clean package
```

## Current MVP Features

* Kafka ingestion
* Flink stream processing
* IoTDB persistence
* Grafana dashboards
* Docker deployment
* End-to-end smoke testing

## Next Phase

Production hardening:

* Flink checkpoints
* Savepoints
* Monitoring & alerting
* Integration tests
* CI/CD pipeline
* Kubernetes deployment

## License

TBD
