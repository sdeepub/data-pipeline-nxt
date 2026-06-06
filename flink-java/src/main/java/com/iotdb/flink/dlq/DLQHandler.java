package com.iotdb.flink.dlq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.ExceptionUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;

/**
 * DLQHandler
 *
 * Routes malformed, unparseable, or failed records to a Dead Letter Queue (DLQ) topic.
 *
 * Responsibilities:
 * - Wrap errors with metadata (deviceId, timestamp, error message, stack trace)
 * - Send to Kafka DLQ topic for post-mortem analysis
 * - Preserve original input for debugging
 * - Never block the main pipeline
 * - Log suspicious patterns (e.g., same error 1000x from same device)
 *
 * Message format (to DLQ topic):
 * {
 *   "timestamp": "2026-06-06T12:34:56Z",
 *   "source": "telemetry-parser|edge-trimming|normalizer",
 *   "error_type": "JSON_PARSE_ERROR|SCHEMA_VALIDATION_ERROR|TYPE_CONVERSION_ERROR",
 *   "error_message": "Invalid JSON: ...",
 *   "stack_trace": "...",
 *   "original_input": "raw string that failed",
 *   "device_id": "MC001" (if extractable),
 *   "request_id": "uuid-for-tracing"
 * }
 *
 * Usage in pipeline:
 *   DataStream<TelemetryEvent> validated = raw
 *       .map(new TelemetryParser())
 *       .process(new DLQWrapper(dlqHandler, "telemetry-parser"));
 *
 *   dlqHandler.addDLQSink(env, "sensor-topic-dlq");
 */
public class DLQHandler implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(DLQHandler.class);
    private static final long serialVersionUID = 1L;

    private final String kafkaBootstrap;
    private final String dlqTopic;
    private static final int DLQ_BUFFER_FLUSH_INTERVAL_MS = 5000;

    // ─────────────────────────────────────────────────────────────────────────
    // Constructor
    // ─────────────────────────────────────────────────────────────────────────

    public DLQHandler(String kafkaBootstrap, String dlqTopic) {
        this.kafkaBootstrap = kafkaBootstrap;
        this.dlqTopic = dlqTopic;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Create DLQ message from exception
    // ─────────────────────────────────────────────────────────────────────────

    public DLQMessage createDLQMessage(String source,
                                       String errorType,
                                       String errorMessage,
                                       String originalInput,
                                       Optional<String> deviceId,
                                       Throwable cause) {

        DLQMessage msg = new DLQMessage();
        msg.timestamp = Instant.now().toString();
        msg.source = source;
        msg.errorType = errorType;
        msg.errorMessage = errorMessage;
        msg.originalInput = truncate(originalInput, 2048); // limit size
        msg.stackTrace = cause != null ? ExceptionUtils.stringifyException(cause) : null;
        msg.deviceId = deviceId.orElse(null);
        msg.requestId = java.util.UUID.randomUUID().toString();

        return msg;
    }

    /**
     * Log a DLQ event (also sends to Kafka, but this logs to application logs).
     * Use for alerts/warnings.
     */
    public void logDLQEvent(DLQMessage msg) {
        log.warn("[DLQ] {} | {} | Input: {} | Device: {} | RequestId: {}",
                msg.source,
                msg.errorType,
                truncate(msg.originalInput, 100),
                msg.deviceId != null ? msg.deviceId : "UNKNOWN",
                msg.requestId
        );

        if (log.isDebugEnabled()) {
            log.debug("[DLQ] Stack trace for requestId {}: {}",
                    msg.requestId, msg.stackTrace);
        }
    }

    /**
     * Serialize DLQMessage to JSON for Kafka.
     */
    public String serializeToJSON(DLQMessage msg) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(msg);
        } catch (Exception e) {
            // Fallback if serialization fails
            return String.format(
                    "{\"error\":\"Serialization failed\",\"original_error\":\"%s\",\"requestId\":\"%s\"}",
                    e.getMessage(),
                    msg.requestId
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Kafka sink for DLQ
    // ─────────────────────────────────────────────────────────────────────────

    public FlinkKafkaProducer<DLQMessage> createDLQSink() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBootstrap);
        props.setProperty("acks", "1"); // faster than "all"
        props.setProperty("retries", "3");
        props.setProperty("linger.ms", "100"); // batch for efficiency

        return new FlinkKafkaProducer<>(
                dlqTopic,
                new DLQSerializationSchema(this),
                props,
                FlinkKafkaProducer.Semantic.NONE // at-most-once (DLQ doesn't need exactly-once)
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper
    // ─────────────────────────────────────────────────────────────────────────

    private String truncate(String str, int maxLength) {
        if (str == null) return null;
        if (str.length() <= maxLength) return str;
        return str.substring(0, maxLength) + "...";
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DLQMessage DTO
    // ─────────────────────────────────────────────────────────────────────────

    public static class DLQMessage implements Serializable {
        private static final long serialVersionUID = 1L;

        public String timestamp;           // ISO-8601
        public String source;              // "telemetry-parser", "normalizer", etc.
        public String errorType;           // "JSON_PARSE_ERROR", "VALIDATION_ERROR", etc.
        public String errorMessage;        // human-readable error
        public String stackTrace;          // full exception stack
        public String originalInput;       // what was being processed (truncated)
        public String deviceId;            // extracted if possible
        public String requestId;           // unique ID for tracing

        @Override
        public String toString() {
            return String.format(
                    "DLQMessage[%s | %s | %s | RequestId: %s]",
                    source, errorType, errorMessage, requestId
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Kafka serialization schema
    // ─────────────────────────────────────────────────────────────────────────

    private static class DLQSerializationSchema implements KafkaSerializationSchema<DLQMessage> {

        private final DLQHandler handler;

        DLQSerializationSchema(DLQHandler handler) {
            this.handler = handler;
        }

        @Override
        public void open(org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext context) {
            // No-op
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(DLQMessage element, Long timestamp) {
            String json = handler.serializeToJSON(element);
            byte[] valueBytes = json.getBytes(StandardCharsets.UTF_8);

            // Use deviceId as key for Kafka partitioning (all errors from same device go to same partition)
            byte[] keyBytes = element.deviceId != null
                    ? element.deviceId.getBytes(StandardCharsets.UTF_8)
                    : null;

            return new ProducerRecord<>(handler.dlqTopic, keyBytes, valueBytes);
        }
    }
}

/**
 * DLQWrapper
 *
 * Wraps a MapFunction and catches exceptions, sending them to DLQ.
 * Usage:
 *   DataStream<Output> result = input
 *       .map(new DLQWrapper<>(
 *           new TelemetryParser(),
 *           dlqHandler,
 *           "telemetry-parser"
 *       ));
 */
abstract class DLQWrapper<IN, OUT> implements org.apache.flink.api.common.functions.MapFunction<IN, OUT> {

    protected final DLQHandler dlqHandler;
    protected final String source;

    protected DLQWrapper(DLQHandler dlqHandler, String source) {
        this.dlqHandler = dlqHandler;
        this.source = source;
    }

    @Override
    public final OUT map(IN value) throws Exception {
        try {
            return mapInner(value);
        } catch (Exception e) {
            // Extract error details
            String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            String errorType = e.getClass().getSimpleName();
            String originalInput = value != null ? value.toString() : "null";
            Optional<String> deviceId = extractDeviceId(value);

            // Create and send DLQ message
            DLQHandler.DLQMessage msg = dlqHandler.createDLQMessage(
                    source,
                    errorType,
                    errorMessage,
                    originalInput,
                    deviceId,
                    e
            );
            dlqHandler.logDLQEvent(msg);

            // Re-throw or return null (depending on your pipeline requirements)
            // For now, return null to let the record be skipped
            return null;
        }
    }

    /**
     * Subclasses implement this.
     */
    protected abstract OUT mapInner(IN value) throws Exception;

    /**
     * Attempt to extract deviceId from input for better tracing.
     * Override in subclasses.
     */
    protected Optional<String> extractDeviceId(IN value) {
        return Optional.empty();
    }
}

/**
 * Example: TelemetryParserWithDLQ
 *
 * Concrete implementation wrapping TelemetryParser with DLQ support.
 */
class TelemetryParserWithDLQ extends DLQWrapper<String, String> {

    private static final Logger log = LoggerFactory.getLogger(TelemetryParserWithDLQ.class);

    private final ObjectMapper mapper = new ObjectMapper();

    public TelemetryParserWithDLQ(DLQHandler dlqHandler) {
        super(dlqHandler, "telemetry-parser");
    }

    @Override
    protected String mapInner(String json) throws Exception {
        // Parse and validate
        JsonNode node = mapper.readTree(json);

        // Check required fields
        if (!node.has("deviceId")) {
            throw new IllegalArgumentException("Missing required field: deviceId");
        }
        if (!node.has("ts")) {
            throw new IllegalArgumentException("Missing required field: ts");
        }

        // Validate timestamp is long
        if (!node.get("ts").isNumber()) {
            throw new IllegalArgumentException("Field 'ts' must be numeric");
        }

        return json; // Return parsed JSON (or deserialized object)
    }

    @Override
    protected Optional<String> extractDeviceId(String value) {
        try {
            JsonNode node = mapper.readTree(value);
            if (node.has("deviceId")) {
                return Optional.of(node.get("deviceId").asText());
            }
        } catch (Exception e) {
            // Ignore
        }
        return Optional.empty();
    }
}
