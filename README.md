# Data Pipeline NxT

A high-performance Java rewrite of the legacy Python data pipeline. Built with Apache Flink, Kafka, and IoTDB for scalable, real-time IoT data ingestion and analytics.

## MVP: Functional End-to-End Pipeline

**Status**: 70% Functional (Core components working, needs validation & monitoring)

### MVP Scope

Process IoT sensor data in real-time from multiple machines:
1. **Ingest** sensor telemetry via Kafka message broker
2. **Process** events in Flink (JSON deserialization, validation)
3. **Store** time-series data in Apache IoTDB
4. **Visualize** metrics and trends in Grafana dashboard

### Success Metrics

| Metric | Target | Current |
|--------|--------|---------|
| **Throughput** | 1,000+ events/sec | Testing required |
| **Latency (p99)** | < 500ms end-to-end | Testing required |
| **Data Accuracy** | 100% (no data loss) | Testing required |
| **Availability** | 99% uptime | Testing required |
| **Visualization** | Real-time dashboard in Grafana | Configured, needs validation |

---

## Architecture

```
┌─────────────────┐
│  Simulator (10  │ Generates 10 machines
│  machines)      │ × 7 metrics every 2s
└────────┬────────┘
         │ JSON events (Kafka)
         ▼
┌─────────────────────────────┐
│   Apache Kafka (Zookeeper)  │ Message broker
│   Topic: sensor-topic       │ Partition: 1 (scale later)
│   DLQ: sensor-topic-dlq     │
└────────┬────────────────────┘
         │ Consume with Flink
         ▼
┌─────────────────────────────┐
│  Flink Streaming Pipeline   │ Process & enrich
│  - JSON deserialization     │ (modular subsystems)
│  - Type mapping             │
│  - Error handling (DLQ)     │
└────────┬────────────────────┘
         │ Insert records
         ▼
┌─────────────────────────────┐
│    Apache IoTDB (1.3.0)      │ Time-series storage
│    Storage Group:           │ 7 measurements per device
│    root.factory1.*          │
└────────┬────────────────────┘
         │ REST API (port 8080)
         ▼
┌─────────────────────────────┐
│  Grafana (3000)             │ Real-time dashboard
│  IoTDB datasource plugin    │
└─────────────────────────────┘
```

### Sensor Data Schema

```json
{
  "machine_id": "MC001",
  "machine_type": "CNC_MILL",
  "location": "FACTORY_A",
  "timestamp": 1717769347000,
  "gas_temperature": 72.5,
  "gas_pressure": 45.2,
  "humidity": 55.8,
  "spin_rate": 1200.0,
  "torque": 85.3,
  "status": "RUNNING",
  "fault_code": "0x00"
}
```

---

## Quick Start

### Prerequisites

- Docker & Docker Compose (v3.8+)
- 4GB RAM minimum
- Ports: 2181, 9092, 6667, 8081, 8080, 3000

### Run the Pipeline

```bash
# Clone and navigate
git clone https://github.com/sdeepub/data-pipeline-nxt.git
cd data-pipeline-nxt

# Start all services
docker compose up -d

# Wait for services to be healthy (30-60 seconds)
docker compose ps  # Check STATUS column

# Scale Flink TaskManagers (optional)
docker compose up -d --scale flink-taskmanager=2
```

### Verify Pipeline is Working

```bash
# 1. Check Flink JobManager UI
curl http://localhost:8081/v1/overview | jq .

# 2. Check IoTDB has data
docker exec iotdb iotdb-sql.sh -h 127.0.0.1 -p 6667 -u root -pw root \
  -e "SELECT COUNT(*) FROM root.factory1.** WHERE TIME > 0;"

# 3. Open Grafana Dashboard
# URL: http://localhost:3000
# User: admin / admin
# (Dashboard pre-configured with IoTDB datasource)
```

### Stop the Pipeline

```bash
docker compose down -v  # -v removes volumes for clean restart
```

---

## Development

### Project Structure

```
data-pipeline-nxt/
├── flink-java/                          # Main pipeline (Java/Maven)
│   ├── src/main/java/com/iotdb/flink/
│   │   ├── KafkaToIoTDB.java            # Entry point
│   │   ├── SensorData.java              # Data model (POJO)
│   │   ├── ddl/                         # DDL executor (future)
│   │   ├── dlq/                         # Dead letter queue handler (future)
│   │   ├── manifest/                    # Manifest validator (future)
│   │   └── operators/                   # Custom Flink operators (future)
│   ├── pom.xml                          # Maven dependencies
│   └── Dockerfile                       # Multi-stage build
├── simulators/                          # Data generators
│   ├── machine_simulator.py             # Sensor data simulator
│   ├── requirements.txt
│   └── Dockerfile
├── grafana/                             # Dashboard & configs
├── iotdb/                               # (Reserved for future)
├── k8s/                                 # (Reserved for Kubernetes)
├── docker-compose.yml                   # Orchestration
└── README.md                            # This file
```

### Build Locally (Dev)

```bash
cd flink-java

# Build FAT JAR
mvn clean package

# Result: target/kafka-iotdb-pipeline.jar (all dependencies included)
```

### Code Guidelines

- **Language**: Java 11+
- **Build Tool**: Maven 3.9+
- **Style**: Google Java Style Guide (run formatter if available)
- **Testing**: JUnit 5 (to be added, see Issue #14)

---

## Subsystems (Production Features)

These are implemented but not yet integrated:

| Subsystem | Purpose | Status | Issue |
|-----------|---------|--------|-------|
| **DDL Executor** | Auto-create IoTDB schema from events | ✅ Code ready | #8 |
| **Manifest Validator** | Validate events against manifest | ✅ Code ready | #8 |
| **DLQ Handler** | Route failed events to Dead Letter Queue | ✅ Code ready | #8 |
| **Edge Trimming** | Drop old metrics to save storage | ✅ Code ready | #8 |

---

## Operational Dashboards & Commands

### Kafka Topics

```bash
# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Monitor main topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic sensor-topic --from-beginning | jq .

# Monitor DLQ (errors)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic sensor-topic-dlq --from-beginning
```

### Flink Web UI

- **URL**: http://localhost:8081
- **Jobs**: View running jobs, parallelism, task managers
- **Metrics**: CPU, memory, throughput per operator
- **Logs**: Troubleshoot failures

### IoTDB CLI

```bash
docker exec iotdb iotdb-sql.sh -h 127.0.0.1 -p 6667 -u root -pw root

# Once in shell:
> SHOW DATABASES;
> SELECT * FROM root.factory1.MC001 LIMIT 10;
> SELECT COUNT(*) FROM root.factory1.** WHERE TIME > now() - 1h;
```

### Grafana

- **URL**: http://localhost:3000
- **Datasource**: Apache IoTDB (pre-configured)
- **Dashboards**: Import from `grafana/provisioning/dashboards/` (if present)

---

## Troubleshooting

### Flink Job Not Submitting

```bash
# Check flink-job container logs
docker compose logs flink-job

# Manually submit (debugging)
docker exec flink-jobmanager flink run -m localhost:8081 \
  --jarfile /opt/flink/usrlib/kafka-iotdb-pipeline.jar \
  -c com.iotdb.flink.KafkaToIoTDB
```

### No Data in IoTDB

```bash
# 1. Check simulator is running
docker compose logs simulator

# 2. Check Kafka has events
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic sensor-topic --max-messages 5

# 3. Check Flink logs for parse errors
docker compose logs flink-jobmanager
docker compose logs flink-job

# 4. Check IoTDB connectivity
docker exec flink-jobmanager curl http://iotdb:8080/ping
```

### Memory/Performance Issues

```bash
# Increase Flink TaskManager memory (docker-compose.yml)
# taskmanager.memory.process.size: 1024m → 2048m

# Reduce parallelism
# parallelism.default: 4 → 2

# Restart
docker compose down && docker compose up -d
```

---

## Next Steps (Roadmap)

### Phase 2: Production Hardening (Sprint 2-3)
- [ ] Issue #9: Refactor into modular components
- [ ] Issue #10: Containerize & publish images
- [ ] Issue #11: Add orchestration (Airflow/Prefect)
- [ ] Issue #12: Implement retries & dependency handling

### Phase 3: Quality & Reliability (Sprint 4-5)
- [ ] Issue #13: Add integration tests
- [ ] Issue #14: Expand unit test coverage to 70%
- [ ] Issue #15: Data quality checks (Great Expectations)
- [ ] Issue #16: Integrate secrets manager
- [ ] Issue #18: Security checklist

### Phase 4: Operations (Sprint 6)
- [ ] Issue #19: Add metrics, tracing, logging
- [ ] Issue #20: Load testing & autoscaling
- [ ] Issue #21: Onboarding docs & runbook
- [ ] Issue #22: Beta program & user feedback

---

## Contributing

1. Create a feature branch: `git checkout -b feature/issue-123`
2. Follow [Conventional Commits](https://www.conventionalcommits.org/): `feat: description`
3. Open a Pull Request with a clear description
4. Code review + tests required before merge

## License

TBD

## Support

- **Issues**: [GitHub Issues](https://github.com/sdeepub/data-pipeline-nxt/issues)
- **Discussions**: [GitHub Discussions](https://github.com/sdeepub/data-pipeline-nxt/discussions)
