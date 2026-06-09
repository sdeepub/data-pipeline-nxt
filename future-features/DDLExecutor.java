package com.iotdb.flink.ddl;

import com.iotdb.flink.manifest.Manifest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DDLExecutor
 *
 * Single-threaded (parallelism=1) sink that applies schema DDLs to IoTDB.
 *
 * Responsibilities:
 * - Create storage groups if not exist
 * - Create templates from manifests
 * - Create timeseries for specific devices
 * - Batch DDL operations for efficiency
 * - Idempotent: safe to run multiple times
 * - Comprehensive error handling & logging
 * - Track applied DDLs to avoid duplicates
 *
 * Deployment:
 *   normalized
 *     .map(new DDLRequestMapper())  // extract device path + measurements
 *     .addSink(new DDLExecutor())
 *     .setParallelism(1)  // CRITICAL: single parallelism only
 *     .name("ddl-executor");
 *
 * Configuration (environment variables):
 *   IOTDB_HOST
 *   IOTDB_PORT
 *   IOTDB_USER
 *   IOTDB_PASSWORD
 *   DDL_BATCH_SIZE (default: 100)
 *   DDL_TIMEOUT_MS (default: 30000)
 */
public class DDLExecutor extends RichSinkFunction<DDLRequest> {

    private static final Logger log = LoggerFactory.getLogger(DDLExecutor.class);

    // Configuration
    private final String iotdbHost;
    private final int iotdbPort;
    private final String iotdbUser;
    private final String iotdbPassword;
    private final int batchSize;
    private final int timeoutMs;

    // Runtime state
    private transient Connection conn;
    private final Queue<DDLRequest> batch = new LinkedList<>();
    private final Set<String> appliedDDLs = ConcurrentHashMap.newKeySet(); // track applied DDLs
    private final Map<String, Long> lastAttempt = new ConcurrentHashMap<>(); // prevent retry storms

    private static final long MIN_RETRY_INTERVAL_MS = 5000; // don't retry same DDL too quickly

    // ─────────────────────────────────────────────────────────────────────────
    // Constructor
    // ─────────────────────────────────────────────────────────────────────────

    public DDLExecutor() {
        this.iotdbHost = getEnv("IOTDB_HOST", "iotdb");
        this.iotdbPort = Integer.parseInt(getEnv("IOTDB_PORT", "6667"));
        this.iotdbUser = getEnv("IOTDB_USER", "root");
        this.iotdbPassword = getEnv("IOTDB_PASSWORD", "root");
        this.batchSize = Integer.parseInt(getEnv("DDL_BATCH_SIZE", "100"));
        this.timeoutMs = Integer.parseInt(getEnv("DDL_TIMEOUT_MS", "30000"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("[DDLExecutor] Opening connection to IoTDB {}:{}",
                iotdbHost, iotdbPort);

        try {
            Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
            conn = DriverManager.getConnection(
                    String.format("jdbc:iotdb://%s:%d/", iotdbHost, iotdbPort),
                    iotdbUser,
                    iotdbPassword
            );
            conn.setNetworkTimeout(null, timeoutMs);
            log.info("[DDLExecutor] Connected to IoTDB successfully");
        } catch (Exception e) {
            log.error("[DDLExecutor] Failed to connect to IoTDB", e);
            throw e;
        }
    }

    @Override
    public void invoke(DDLRequest request, Context context) throws Exception {
        if (request == null) {
            return;
        }

        // Add to batch
        batch.offer(request);

        // Flush batch if full
        if (batch.size() >= batchSize) {
            flushBatch();
        }
    }

    @Override
    public void close() throws Exception {
        // Final flush
        try {
            if (!batch.isEmpty()) {
                flushBatch();
            }
        } catch (Exception e) {
            log.error("[DDLExecutor] Error during final flush", e);
        }

        // Close connection
        if (conn != null && !conn.isClosed()) {
            conn.close();
            log.info("[DDLExecutor] Connection closed");
        }

        super.close();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Batch processing
    // ─────────────────────────────────────────────────────────────────────────

    private synchronized void flushBatch() throws SQLException {
        if (batch.isEmpty()) {
            return;
        }

        int processed = 0;
        int skipped = 0;
        int failed = 0;

        while (!batch.isEmpty()) {
            DDLRequest req = batch.poll();
            String ddlKey = req.getDDLKey(); // unique identifier for this DDL

            // Skip if already applied
            if (appliedDDLs.contains(ddlKey)) {
                skipped++;
                continue;
            }

            // Skip if we tried recently and it failed
            Long lastAttemptTime = lastAttempt.get(ddlKey);
            if (lastAttemptTime != null && 
                System.currentTimeMillis() - lastAttemptTime < MIN_RETRY_INTERVAL_MS) {
                skipped++;
                continue;
            }

            // Execute DDL
            try {
                executeDDL(req);
                appliedDDLs.add(ddlKey);
                processed++;
            } catch (SQLException e) {
                // Log but don't fail the whole pipeline
                if (isAlreadyExistsError(e)) {
                    // This is expected (idempotency)
                    log.debug("[DDLExecutor] DDL already applied (expected): {}", ddlKey);
                    appliedDDLs.add(ddlKey);
                    processed++;
                } else {
                    log.warn("[DDLExecutor] Failed to execute DDL {}: {}",
                            ddlKey, e.getMessage());
                    lastAttempt.put(ddlKey, System.currentTimeMillis());
                    failed++;
                }
            }
        }

        if (processed > 0 || failed > 0) {
            log.info("[DDLExecutor] Batch flush: {} processed, {} skipped, {} failed",
                    processed, skipped, failed);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DDL execution
    // ─────────────────────────────────────────────────────────────────────────

    private void executeDDL(DDLRequest req) throws SQLException {
        String ddl = req.buildDDL();
        log.debug("[DDLExecutor] Executing: {}", ddl);

        try (Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(timeoutMs / 1000); // seconds
            stmt.execute(ddl);
        } catch (SQLTimeoutException e) {
            log.error("[DDLExecutor] DDL timeout: {}", ddl);
            throw e;
        } catch (SQLException e) {
            log.error("[DDLExecutor] DDL failed: {} | Error: {}",
                    ddl, e.getMessage());
            throw e;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper: detect idempotent errors
    // ─────────────────────────────────────────────────────────────────────────

    private boolean isAlreadyExistsError(SQLException e) {
        // IoTDB error codes for "already exists"
        String msg = e.getMessage().toLowerCase();
        return msg.contains("already exist") || 
               msg.contains("existed") ||
               msg.contains("duplicate");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Utilities
    // ─────────────────────────────────────────────────────────────────────────

    private String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DDLRequest inner class
    // ─────────────────────────────────────────────────────────────────────────

    public static class DDLRequest {
        public enum Type {
            CREATE_STORAGE_GROUP,
            CREATE_TEMPLATE,
            CREATE_TIMESERIES,
            APPLY_TEMPLATE
        }

        public Type type;
        public String storageGroup;
        public String devicePath;
        public String measurement;
        public String dataType;
        public String encoding;
        public String compression;

        // Additional fields for templates
        public String templateName;
        public List<Manifest.Measurement> measurements;

        // Unique identifier for deduplication
        public String getDDLKey() {
            return switch (type) {
                case CREATE_STORAGE_GROUP -> "SG:" + storageGroup;
                case CREATE_TEMPLATE -> "TPL:" + templateName;
                case CREATE_TIMESERIES -> "TS:" + devicePath + "." + measurement;
                case APPLY_TEMPLATE -> "APPLY:" + devicePath + ":" + templateName;
            };
        }

        public String buildDDL() {
            return switch (type) {
                case CREATE_STORAGE_GROUP ->
                        String.format("SET STORAGE GROUP TO %s", storageGroup);

                case CREATE_TEMPLATE ->
                        buildCreateTemplateDDL();

                case CREATE_TIMESERIES ->
                        String.format(
                                "CREATE TIMESERIES IF NOT EXISTS %s.%s WITH DATATYPE=%s, ENCODING=%s, COMPRESSOR=%s",
                                devicePath, measurement, dataType, encoding, compression
                        );

                case APPLY_TEMPLATE ->
                        String.format("CREATE ALIGNED TIMESERIES %s USING TEMPLATE %s",
                                devicePath, templateName);
            };
        }

        private String buildCreateTemplateDDL() {
            if (measurements == null || measurements.isEmpty()) {
                throw new IllegalArgumentException("No measurements for template");
            }

            StringBuilder sb = new StringBuilder("CREATE TEMPLATE ");
            sb.append(templateName).append(" ALIGNED (");

            for (int i = 0; i < measurements.size(); i++) {
                Manifest.Measurement m = measurements.get(i);
                if (i > 0) sb.append(", ");
                sb.append(m.name).append(" ").append(m.dataType);
                if (m.encoding != null) {
                    sb.append(" ENCODING=").append(m.encoding);
                }
                if (m.compression != null) {
                    sb.append(" COMPRESSION=").append(m.compression);
                }
            }

            sb.append(")");
            return sb.toString();
        }

        @Override
        public String toString() {
            return String.format("DDLRequest[%s: %s]", type, getDDLKey());
        }
    }
}
