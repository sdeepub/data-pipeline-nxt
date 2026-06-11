package com.iotdb.flink;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.CheckpointingMode;

import java.util.Properties;
import java.util.UUID;

/**
 * KafkaToIoTDB FINAL: Lightweight Guard Pipeline
 * 
 * ─────────────────────────────────────────────────────────────
 * ARCHITECTURE:
 * ─────────────────────────────────────────────────────────────
 * 
 * INPUT TOPICS (Kafka):
 *   ├─ sensor-topic       → Telemetry data (high volume)
 *   ├─ context-topic      → Trackin/trackout events
 *   └─ rules-topic        → Rule configuration (broadcast)
 * 
 * PROCESSING (Flink - Stateless Guard):
 *   ├─ Parse all three sources
 *   ├─ Broadcast rules (in-memory)
 *   ├─ Sequence guard: detect implicit run close
 *   │  (trackin without trackout → flag but don't block)
 *   ├─ Threshold guard: check sensor values
 *   ├─ Add evaluation flags: passed (true/false) + alert details
 *   └─ Add audit trail: context_anomaly (for DLQ)
 * 
 * OUTPUT STREAMS (Three Sinks):
 *   ├─ Sink 1: IoTDB Bronze (ALL raw data)
 *   ├─ Sink 2: evaluation-topic (ALL data + flags + anomalies)
 *   └─ Sink 3: dlq-topic (anomalies for reconciliation)
 * 
 * DOWNSTREAM (Dagster):
 *   ├─ IF alert (passed=false) → Immediate alert handler
 *   ├─ IF context_anomaly → Audit stream + reconciliation
 *   └─ IF passed=true → Scheduled aggregation (end of shift)
 * 
 * KEY FEATURES:
 *   ✅ Configuration-driven (no hard-coded rules)
 *   ✅ Forgiving sequence logic (implicit close, don't block)
 *   ✅ Full audit trail (DLQ captures anomalies)
 *   ✅ Reconciliation-ready (Dagster can match delayed trackout)
 *   ✅ Savepoint support (safe production upgrades)
 *   ✅ High throughput (stateless, parallelizable)
 * 
 * ─────────────────────────────────────────────────────────────
 */
public class KafkaToIoTDB_FINAL {

    public static void main(String[] args) throws Exception {

        // ────────────────────────────────────────────────────────────
        // 1. ENVIRONMENT SETUP
        // ────────────────────────────────────────────────────────────
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(8);  // Stateless = high parallelism

        // Checkpointing & Savepoints
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Enable externalized checkpoints for production-safe restarts
        env.getCheckpointConfig().enableExternalizedCheckpoints(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
	
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");

        // ────────────────────────────────────────────────────────────
        // 2. KAFKA CONFIGURATION
        // ────────────────────────────────────────────────────────────
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka:29092");
        kafkaProps.setProperty("group.id", "lightweight-guard");
        kafkaProps.setProperty("auto.offset.reset", "latest");

        // ────────────────────────────────────────────────────────────
        // 3. KAFKA SOURCES
        // ────────────────────────────────────────────────────────────

        // Source 1: Telemetry (sensor data, high volume)
        DataStream<String> telemetryRaw = env.addSource(
            new FlinkKafkaConsumer<>(
                "sensor-topic",
                new SimpleStringSchema(),
                kafkaProps
            )
        ).name("telemetry-source")
         .setParallelism(8);

        // Source 2: Context (trackin/trackout events)
        DataStream<String> contextRaw = env.addSource(
            new FlinkKafkaConsumer<>(
                "context-topic",
                new SimpleStringSchema(),
                kafkaProps
            )
        ).name("context-source")
         .setParallelism(4);

        // Source 3: Rules (broadcast configuration)
        DataStream<String> rulesRaw = env.addSource(
            new FlinkKafkaConsumer<>(
                "rules-topic",
                new SimpleStringSchema(),
                kafkaProps
            )
        ).name("rules-source")
         .setParallelism(1);

        // ────────────────────────────────────────────────────────────
        // 4. PARSE SOURCES
        // ────────────────────────────────────────────────────────────

        DataStream<SensorData> telemetryParsed = telemetryRaw
            .map(new RichMapFunction<String, SensorData>() {
                private transient ObjectMapper mapper;
                @Override
                public void open(Configuration params) throws Exception {
                    this.mapper = new ObjectMapper();
                }
                @Override
                public SensorData map(String json) throws Exception {
                    return mapper.readValue(json, SensorData.class);
                }
            })
            .name("parse-telemetry");

        DataStream<ContextEvent> contextParsed = contextRaw
            .map(new RichMapFunction<String, ContextEvent>() {
                private transient ObjectMapper mapper;
                @Override
                public void open(Configuration params) throws Exception {
                    this.mapper = new ObjectMapper();
                }
                @Override
                public ContextEvent map(String json) throws Exception {
                    return mapper.readValue(json, ContextEvent.class);
                }
            })
            .name("parse-context");

        DataStream<Rule> rulesParsed = rulesRaw
            .map(new RichMapFunction<String, Rule>() {
                private transient ObjectMapper mapper;
                @Override
                public void open(Configuration params) throws Exception {
                    this.mapper = new ObjectMapper();
                }
                @Override
                public Rule map(String json) throws Exception {
                    return mapper.readValue(json, Rule.class);
                }
            })
            .name("parse-rules");

        // ────────────────────────────────────────────────────────────
        // 5. BROADCAST RULES (Configuration-Driven)
        // ────────────────────────────────────────────────────────────

        MapStateDescriptor<String, Rule> rulesStateDesc =
            new MapStateDescriptor<>("rules", String.class, Rule.class);

        BroadcastStream<Rule> rulesBroadcast = rulesParsed
            .broadcast(rulesStateDesc);

        // ────────────────────────────────────────────────────────────
        // 6. LIGHTWEIGHT GUARD: Evaluate (Sequence + Threshold)
        // ────────────────────────────────────────────────────────────

        KeyedStream<SensorData, String> telemetryByDevice = telemetryParsed
            .keyBy(SensorData::getMachine_id);

        KeyedStream<ContextEvent, String> contextByDevice = contextParsed
            .keyBy(ContextEvent::getDevice_id);

        // Guard: Connect telemetry + context + rules
        // CORRECT - simplified for Flink 1.18.1
	// DataStream<EvaluationResult> evaluated = telemetryByDevice
	//     .connect(contextByDevice)
	//     .process(new LightweightGuardFunction(rulesStateDesc))
	//     .broadcast(rulesBroadcast)  // Broadcast AFTER process
	//     .name("lightweight-guard");

	DataStream<ContextAwareTelemetry> contextAware =
	    telemetryByDevice
	    .connect(contextByDevice)
	    .process(new ContextJoinFunction());

	DataStream<EvaluationResult> evaluated =
	    contextAware
	    .keyBy(ContextAwareTelemetry::getDeviceId)
	    .connect(rulesBroadcast)
	    .process(new RuleEvaluationFunction(rulesStateDesc));

        // ────────────────────────────────────────────────────────────
        // 7. OUTPUT SINK 1: IoTDB Bronze (ALL raw data)
        // ────────────────────────────────────────────────────────────

        evaluated
            .map(result -> {
                // Convert EvaluationResult back to SensorData for IoTDB
                SensorData sensor = new SensorData();
                sensor.setMachine_id(result.device_id);
                sensor.setTimestamp(result.timestamp);
                sensor.setGas_temperature(result.gas_temperature);
                sensor.setGas_pressure(result.gas_pressure);
                sensor.setHumidity(result.humidity);
                sensor.setSpin_rate(result.spin_rate);
                sensor.setTorque(result.torque);
                sensor.setStatus(result.status);
                sensor.setFault_code(result.fault_code);
                return sensor;
            })
            .name("map-to-iotdb-format")
            .addSink(new SimpleIoTDBTabletSink("root.factory1"))
            .setParallelism(4)
            .name("iotdb-bronze-sink");

        // ────────────────────────────────────────────────────────────
        // 8. OUTPUT SINK 2: Kafka evaluation-topic (with flags)
        // ────────────────────────────────────────────────────────────

        evaluated
            .map(new RichMapFunction<EvaluationResult, String>() {
                private transient ObjectMapper mapper;
                @Override
                public void open(Configuration params) throws Exception {
                    this.mapper = new ObjectMapper();
                }
                @Override
                public String map(EvaluationResult result) throws Exception {
                    return mapper.writeValueAsString(result);
                }
            })
            .name("serialize-evaluation")
            .addSink(new FlinkKafkaProducer<>(
                "evaluation-topic",
                new SimpleStringSchema(),
                kafkaProps
            ))
            .setParallelism(4)
            .name("evaluation-kafka-sink");

        // ────────────────────────────────────────────────────────────
        // 9. OUTPUT SINK 3: Kafka DLQ-topic (audit trail)
        // ────────────────────────────────────────────────────────────

        evaluated
            .flatMap((EvaluationResult result, org.apache.flink.util.Collector<DLQRecord> out) -> {
                // Only emit to DLQ if there's a context anomaly
                if (result.context_anomaly != null) {
                    DLQRecord dlq = new DLQRecord();
                    dlq.anomaly_id = UUID.randomUUID().toString();
                    dlq.device_id = result.device_id;
                    dlq.anomaly_type = "SEQUENCE_ANOMALY";
                    dlq.context_anomaly = result.context_anomaly;
                    dlq.status = "UNRESOLVED";
                    dlq.resolution_action = "MATCH_WITH_DELAYED_TRACKOUT";
                    dlq.created_ts = System.currentTimeMillis();
                    dlq.resolved_ts = null;

                    out.collect(dlq);

                    System.out.println("[DLQ] Anomaly: " + dlq.anomaly_id 
                        + " (" + dlq.anomaly_type + ") for " + dlq.device_id);
                }
            })
            .name("extract-dlq-records")
            .map(new RichMapFunction<DLQRecord, String>() {
                private transient ObjectMapper mapper;
                @Override
                public void open(Configuration params) throws Exception {
                    this.mapper = new ObjectMapper();
                }
                @Override
                public String map(DLQRecord record) throws Exception {
                    return mapper.writeValueAsString(record);
                }
            })
            .name("serialize-dlq")
            .addSink(new FlinkKafkaProducer<>(
                "dlq-topic",
                new SimpleStringSchema(),
                kafkaProps
            ))
            .setParallelism(2)
            .name("dlq-kafka-sink");

        // ────────────────────────────────────────────────────────────
        // 10. EXECUTE
        // ────────────────────────────────────────────────────────────

        env.execute("LightweightGuard: Kafka → IoTDB + Evaluation + DLQ");
    }
}
