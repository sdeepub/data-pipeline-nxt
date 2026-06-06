# Subsystems Setup Instructions

This guide walks you through integrating the 5 subsystems into your Flink pipeline.

## 📋 Quick Start Checklist

- [ ] Copy Java files to `src/main/java/com/example/iot/flink/`
- [ ] Add Maven dependencies to `pom.xml`
- [ ] Add `manifest-schema.json` to `src/main/resources/`
- [ ] Update Docker Compose to include manifest topic
- [ ] Implement placeholder operators in `IoTDataPipeline_Integration.java`
- [ ] Run integration tests
- [ ] Deploy and monitor

---

## Step 1: Organize Project Structure

```
flink-java/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/example/iot/flink/
│   │   │       ├── IoTDataPipeline_Integration.java (main)
│   │   │       ├── manifest/
│   │   │       │   ├── ManifestValidator.java
│   │   │       │   ├── ManifestStore.java
│   │   │       │   └── Manifest.java
│   │   │       ├── ddl/
│   │   │       │   └── DDLExecutor.java
│   │   │       ├── dlq/
│   │   │       │   └── DLQHandler.java
│   │   │       └── operators/
│   │   │           └── EdgeTrimmingFunction.java
│   │   └── resources/
│   │       └── manifest-schema.json  ← Add this!
│   └── test/
│       └── java/
│           └── com/example/iot/flink/
│               ├── manifest/
│               │   ├── ManifestValidatorTest.java
│               │   └── ManifestStoreTest.java
│               ├── ddl/
│               │   └── DDLExecutorTest.java
│               └── operators/
│                   └── EdgeTrimmingFunctionTest.java
├── pom.xml  ← Update with dependencies
└── Dockerfile
```

---

## Step 2: Update pom.xml

Add these dependencies to your `<dependencies>` section:

```xml
<!-- ============================================================ -->
<!-- Manifest Validation (NEW) -->
<!-- ============================================================ -->

<dependency>
    <groupId>com.networknt</groupId>
    <artifactId>json-schema-validator</artifactId>
    <version>1.0.82</version>
</dependency>

<!-- ============================================================ -->
<!-- Jackson for JSON processing (update version) -->
<!-- ============================================================ -->

<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.17.1</version>
</dependency>

<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-core</artifactId>
    <version>2.17.1</version>
</dependency>

<!-- ============================================================ -->
<!-- Flink (ensure versions match) -->
<!-- ============================================================ -->

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>1.18.1</version>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>3.1.0-1.18</version>
</dependency>

<!-- ============================================================ -->
<!-- Kafka producer (for DLQHandler) -->
<!-- ============================================================ -->

<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.7.0</version>
</dependency>

<!-- ============================================================ -->
<!-- IoTDB for DDL execution -->
<!-- ============================================================ -->

<dependency>
    <groupId>org.apache.iotdb</groupId>
    <artifactId>iotdb-jdbc</artifactId>
    <version>1.3.0</version>
</dependency>

<!-- ============================================================ -->
<!-- Logging -->
<!-- ============================================================ -->

<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>2.0.13</version>
</dependency>

<!-- ============================================================ -->
<!-- Testing -->
<!-- ============================================================ -->

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-test-utils</artifactId>
    <version>1.18.1</version>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>junit</groupId>
    <artifactId>junit</artifactId>
    <version>4.13.2</version>
    <scope>test</scope>
</dependency>
```

### Build plugin (shade plugin for FAT JAR)

Ensure your `<build>` section includes:

```xml
<build>
    <plugins>
        <!-- Maven Compiler -->
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <version>3.11.0</version>
            <configuration>
                <source>11</source>
                <target>11</target>
            </configuration>
        </plugin>

        <!-- Shade plugin (package all dependencies) -->
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.5.1</version>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                    <configuration>
                        <finalName>iot-pipeline</finalName>
                        <transformers>
                            <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                <mainClass>com.example.iot.flink.IoTDataPipeline_Integration</mainClass>
                            </transformer>
                        </transformers>
                        <filters>
                            <filter>
                                <artifact>*:*</artifact>
                                <excludes>
                                    <exclude>META-INF/*.SF</exclude>
                                    <exclude>META-INF/*.DSA</exclude>
                                    <exclude>META-INF/*.RSA</exclude>
                                </excludes>
                            </filter>
                        </filters>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

---

## Step 3: Add manifest-schema.json

Create `src/main/resources/manifest-schema.json` with the JSON schema from the Implementation_Planning.md document.

**Short version:**
```bash
cp docs/schemas/manifest-schema.json src/main/resources/
```

---

## Step 4: Update Docker Compose

Add manifest topic to your Kafka cluster:

```yaml
# In docker-compose.yml

kafka:
  # ... existing config
  environment:
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

# After kafka service, add:

kafka-topics-init:
  image: confluentinc/cp-kafka:7.5.0
  depends_on:
    kafka:
      condition: service_healthy
  entrypoint: bash
  command: |
    -c "
    kafka-topics --create --if-not-exists \
      --bootstrap-server kafka:29092 \
      --topic telemetry --partitions 3 --replication-factor 1
    
    kafka-topics --create --if-not-exists \
      --bootstrap-server kafka:29092 \
      --topic context --partitions 1 --replication-factor 1
    
    kafka-topics --create --if-not-exists \
      --bootstrap-server kafka:29092 \
      --topic manifests --partitions 1 --replication-factor 1
    
    kafka-topics --create --if-not-exists \
      --bootstrap-server kafka:29092 \
      --topic sensor-topic-dlq --partitions 3 --replication-factor 1
    
    kafka-topics --create --if-not-exists \
      --bootstrap-server kafka:29092 \
      --topic alerts --partitions 1 --replication-factor 1
    "
```

---

## Step 5: Implement Placeholder Operators

In `IoTDataPipeline_Integration.java`, implement the stubbed methods:

### TelemetryParserWithValidation
```java
static class TelemetryParserWithValidation 
        extends org.apache.flink.api.common.functions.RichMapFunction<String, TelemetryEvent> {
    
    private final DLQHandler dlqHandler;
    private ObjectMapper mapper;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        mapper = new ObjectMapper();
    }

    @Override
    public TelemetryEvent map(String json) throws Exception {
        try {
            JsonNode node = mapper.readTree(json);
            
            // Validate required fields
            if (!node.has("deviceId") || !node.has("ts") || !node.has("measurements")) {
                throw new IllegalArgumentException("Missing required fields");
            }
            
            TelemetryEvent e = new TelemetryEvent();
            e.deviceId = node.get("deviceId").asText();
            e.ts = node.get("ts").asLong();
            // ... populate measurements
            return e;
            
        } catch (Exception ex) {
            // Send to DLQ
            DLQHandler.DLQMessage msg = dlqHandler.createDLQMessage(
                "telemetry-parser",
                ex.getClass().getSimpleName(),
                ex.getMessage(),
                json,
                java.util.Optional.empty(),
                ex
            );
            dlqHandler.logDLQEvent(msg);
            throw ex;
        }
    }
}
```

### Similar pattern for other operators...

---

## Step 6: Build and Test

```bash
# Compile
mvn clean compile

# Run tests
mvn test

# Create FAT JAR
mvn clean package

# Output
ls -lh target/iot-pipeline.jar
# -rw-r--r-- 1 user group 85M target/iot-pipeline.jar
```

---

## Step 7: Local Integration Test

```bash
# Start services
docker compose up -d kafka zookeeper iotdb grafana

# Wait for health checks
sleep 30

# Submit Flink job
flink run -m localhost:8081 target/iot-pipeline.jar

# Send test data (in another terminal)
python3 -c "
import json
import subprocess
for i in range(100):
    event = {
        'deviceId': f'MC{i:03d}',
        'ts': int(time.time() * 1000),
        'measurements': {'temperature': 22 + i % 10, 'humidity': 50}
    }
    print(json.dumps(event))
" | kafka-console-producer \
    --broker-list localhost:9092 \
    --topic telemetry

# Check IoTDB
iotdb> SELECT * FROM root.factory1.MC001 LIMIT 10;

# Check DLQ (if any errors)
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic sensor-topic-dlq --from-beginning
```

---

## Step 8: Environment Variables for Production

Create `.env` file (or K8s ConfigMap):

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka-broker:29092
KAFKA_TELEMETRY_TOPIC=telemetry
KAFKA_CONTEXT_TOPIC=context
KAFKA_MANIFESTS_TOPIC=manifests
KAFKA_DLQ_TOPIC=telemetry-dlq
KAFKA_ALERTS_TOPIC=alerts

# IoTDB
IOTDB_HOST=iotdb
IOTDB_PORT=6667
IOTDB_USER=root
IOTDB_PASSWORD=root

# DDL Executor
DDL_BATCH_SIZE=100
DDL_TIMEOUT_MS=30000

# Pipeline
FLINK_PARALLELISM=4
FLINK_CHECKPOINT_INTERVAL=60000
FLINK_CHECKPOINT_MODE=EXACTLY_ONCE
```

---

## Step 9: Kubernetes Deployment

Update your Flink JobManager deployment to include environment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager
spec:
  template:
    spec:
      containers:
      - name: flink-jobmanager
        image: flink-iotdb:latest
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: iot-config
              key: kafka.bootstrap
        - name: IOTDB_HOST
          valueFrom:
            configMapKeyRef:
              name: iot-config
              key: iotdb.host
        # ... other env vars
```

---

## Troubleshooting Build Issues

### Error: "json-schema-validator not found"
**Solution**: Maven may not have fetched from central repo
```bash
mvn clean compile -U  # Force update dependencies
```

### Error: "Duplicate class kafka-clients"
**Solution**: Exclude conflicting dependency
```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>3.1.0-1.18</version>
    <exclusions>
        <exclusion>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

### Error: "Class not found: manifest-schema.json"
**Solution**: Ensure it's in the right place AND jar is rebuilt
```bash
ls -la src/main/resources/manifest-schema.json
mvn clean package  # Force rebuild
jar tf target/iot-pipeline.jar | grep manifest-schema
```

---

## Performance Tuning

### For high throughput (1000+ events/sec)

```java
env.setParallelism(8);  // Match CPU cores

// In source operators
telemetryConsumer.setStartFromLatest();

// In flink-conf.yaml
taskmanager.numberOfTaskSlots: 8
taskmanager.memory.process.size: 2g
jobmanager.memory.process.size: 2g
```

### For low latency (<100ms e2e)

```java
env.enableCheckpointing(10_000);  // Every 10s instead of 60s

// In sink operations
iotdb.sink.batch.size = 10  // Smaller batches
ddl.batch.size = 1  // Immediate flush
```

---

## Monitoring & Alerting

### Metrics to track

1. **Pipeline throughput**
   ```
   flink_taskmanager_job_task_operator_records_in_per_sec
   ```

2. **DLQ rate**
   ```
   kafka_topics_records_count{topic="sensor-topic-dlq"}
   ```

3. **Edge trimming events**
   ```
   flink_taskmanager_job_task_operator_records_out_per_sec{operator="edge-trimming"}
   ```

4. **DDL executor latency**
   ```
   flink_taskmanager_job_task_operator_watermark{operator="ddl-executor"}
   ```

### Sample Prometheus query

```promql
# Alert if DLQ rate > 1% of total
(
  rate(kafka_topics_records_count{topic="sensor-topic-dlq"}[5m])
  /
  rate(kafka_topics_records_count{topic="telemetry"}[5m])
) > 0.01
```

---

## Next: Load Testing

Create `src/test/java/LoadTest.java`:

```java
@Test
public void testHighThroughput() throws Exception {
    // Generate 10,000 events/sec for 1 minute
    // Monitor latency distribution (p50, p95, p99)
    // Check IoTDB record count increases
    // Verify no DLQ events
}
```

Run with:
```bash
mvn test -Dtest=LoadTest -Dloadtest=true
```

---

## Support

- **Docs**: See SUBSYSTEMS_GUIDE.md
- **Issues**: Check logs: `kubectl logs -f deployment/flink-jobmanager`
- **Performance**: Run benchmarks with your data sizes
- **Scaling**: Load test before production

**You're ready! 🚀**
