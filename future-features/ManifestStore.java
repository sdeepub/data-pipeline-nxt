package com.iotdb.flink.manifest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ManifestStore
 *
 * Thread-safe store for device-type manifests with versioning and broadcast support.
 *
 * Features:
 * - Stores manifests by deviceType with version tracking
 * - Validates manifests before storing (using ManifestValidator)
 * - Supports snapshot-at-version queries for reproducibility
 * - Thread-safe concurrent updates
 * - Audit trail (who loaded what, when)
 * - Hot-reload support (new version supersedes old)
 *
 * Deployment patterns:
 * - A Flink source reads from config-manifests Kafka topic
 * - ManifestStore receives manifests via updateManifest()
 * - Store broadcasts to all parallel tasks via BroadcastState
 * - Tasks query current manifest by deviceType
 *
 * Usage:
 *   ManifestStore store = new ManifestStore(validator);
 *   ManifestStoreResult result = store.updateManifest(deviceType, jsonNode);
 *   if (result.success()) {
 *     Manifest m = store.getManifest(deviceType);
 *   }
 */
public class ManifestStore {

    private static final Logger log = LoggerFactory.getLogger(ManifestStore.class);

    private final ManifestValidator validator;
    private final ObjectMapper mapper = new ObjectMapper();

    // deviceType → version → ManifestEntry
    private final Map<String, NavigableMap<String, ManifestEntry>> versionedManifests =
            new ConcurrentHashMap<>();

    // deviceType → current version
    private final Map<String, String> currentVersions = new ConcurrentHashMap<>();

    // Audit trail: timestamp → event
    private final List<AuditEvent> auditTrail = Collections.synchronizedList(new ArrayList<>());

    // ─────────────────────────────────────────────────────────────────────────
    // Constructor
    // ─────────────────────────────────────────────────────────────────────────

    public ManifestStore(ManifestValidator validator) {
        this.validator = validator;
        log.info("[ManifestStore] Initialized");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Update manifest (hot-reload entry point)
    // ─────────────────────────────────────────────────────────────────────────

    public ManifestStoreResult updateManifest(String deviceType, JsonNode manifestNode) {
        if (deviceType == null || deviceType.trim().isEmpty()) {
            String msg = "deviceType cannot be null or empty";
            log.error("[ManifestStore] {}", msg);
            return ManifestStoreResult.failure(msg);
        }

        // Validate manifest first
        ManifestValidator.ValidationResult validationResult = validator.validate(manifestNode);
        if (!validationResult.isValid()) {
            String msg = String.format("[ManifestStore] Manifest validation failed for %s:\n  %s",
                    deviceType, validationResult.errorsSummary());
            log.error(msg);
            auditTrail.add(new AuditEvent(
                    Instant.now(),
                    "REJECT",
                    deviceType,
                    "Validation failed: " + validationResult.errors()
            ));
            return ManifestStoreResult.failure(msg);
        }

        // Extract version from manifest
        String version = manifestNode.get("version").asText();

        // Parse manifest to Manifest POJO
        Manifest manifest;
        try {
            manifest = mapper.treeToValue(manifestNode, Manifest.class);
        } catch (Exception e) {
            String msg = "Failed to deserialize manifest: " + e.getMessage();
            log.error("[ManifestStore] {}", msg);
            return ManifestStoreResult.failure(msg);
        }

        // Check for downgrade (new version < current version)
        String currentVersion = currentVersions.get(deviceType);
        if (currentVersion != null && compareVersions(version, currentVersion) < 0) {
            String msg = String.format(
                    "Version downgrade rejected: %s (current) > %s (new) for %s",
                    currentVersion, version, deviceType);
            log.warn("[ManifestStore] {}", msg);
            auditTrail.add(new AuditEvent(
                    Instant.now(),
                    "REJECT",
                    deviceType,
                    "Version downgrade: " + msg
            ));
            return ManifestStoreResult.failure(msg);
        }

        // Check if this version already exists
        boolean isUpdate = false;
        NavigableMap<String, ManifestEntry> versions =
                versionedManifests.computeIfAbsent(deviceType, k -> new TreeMap<>());

        if (versions.containsKey(version)) {
            log.warn("[ManifestStore] Manifest {} version {} already exists (overwriting)",
                    deviceType, version);
            isUpdate = true;
        }

        // Store it
        ManifestEntry entry = new ManifestEntry(
                manifest,
                version,
                Instant.now(),
                manifestNode.toString()
        );
        versions.put(version, entry);
        currentVersions.put(deviceType, version);

        String action = isUpdate ? "UPDATE" : "ACCEPT";
        auditTrail.add(new AuditEvent(
                Instant.now(),
                action,
                deviceType,
                "Version " + version
        ));

        log.info("[ManifestStore] {} Manifest {} v{} | Measurements: {} | Rules: {}",
                action,
                deviceType,
                version,
                manifest.measurements.size(),
                manifest.kpivRules != null ? manifest.kpivRules.size() : 0
        );

        return ManifestStoreResult.success(version, manifest);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Query methods
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Get current manifest for a device type.
     * Returns null if not found.
     */
    public Manifest getManifest(String deviceType) {
        String version = currentVersions.get(deviceType);
        if (version == null) {
            log.warn("[ManifestStore] No manifest found for deviceType: {}", deviceType);
            return null;
        }
        return getManifestByVersion(deviceType, version);
    }

    /**
     * Get manifest for a specific version (for reproducibility/auditing).
     */
    public Manifest getManifestByVersion(String deviceType, String version) {
        NavigableMap<String, ManifestEntry> versions = versionedManifests.get(deviceType);
        if (versions == null) {
            return null;
        }
        ManifestEntry entry = versions.get(version);
        return entry != null ? entry.manifest : null;
    }

    /**
     * Get all known device types.
     */
    public Set<String> getDeviceTypes() {
        return Collections.unmodifiableSet(versionedManifests.keySet());
    }

    /**
     * Get all versions for a device type.
     */
    public List<String> getVersions(String deviceType) {
        NavigableMap<String, ManifestEntry> versions = versionedManifests.get(deviceType);
        if (versions == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(versions.keySet());
    }

    /**
     * Get current version string for a device type.
     */
    public String getCurrentVersion(String deviceType) {
        return currentVersions.get(deviceType);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Audit & diagnostics
    // ─────────────────────────────────────────────────────────────────────────

    public List<AuditEvent> getAuditTrail() {
        return Collections.unmodifiableList(new ArrayList<>(auditTrail));
    }

    public List<AuditEvent> getAuditTrailForDeviceType(String deviceType) {
        return auditTrail.stream()
                .filter(e -> e.deviceType.equals(deviceType))
                .toList();
    }

    public String getStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("ManifestStore Status:\n");
        sb.append("  Device Types: ").append(getDeviceTypes().size()).append("\n");

        for (String deviceType : getDeviceTypes()) {
            String currentVersion = getCurrentVersion(deviceType);
            List<String> allVersions = getVersions(deviceType);
            sb.append("    ").append(deviceType).append(":\n");
            sb.append("      Current version: ").append(currentVersion).append("\n");
            sb.append("      Total versions: ").append(allVersions.size()).append("\n");
        }

        sb.append("  Audit trail entries: ").append(auditTrail.size()).append("\n");
        return sb.toString();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper: Semantic version comparison
    // ─────────────────────────────────────────────────────────────────────────

    private int compareVersions(String v1, String v2) {
        // Simple numeric comparison: "1.2.3" vs "1.2.4"
        // Ignores pre-release/build metadata for simplicity
        String[] p1 = v1.split("[.-]");
        String[] p2 = v2.split("[.-]");

        for (int i = 0; i < Math.min(p1.length, p2.length); i++) {
            try {
                int n1 = Integer.parseInt(p1[i]);
                int n2 = Integer.parseInt(p2[i]);
                if (n1 != n2) {
                    return Integer.compare(n1, n2);
                }
            } catch (NumberFormatException e) {
                // Fallback to string comparison if not numeric
                int cmp = p1[i].compareTo(p2[i]);
                if (cmp != 0) {
                    return cmp;
                }
            }
        }
        return Integer.compare(p1.length, p2.length);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Inner classes
    // ─────────────────────────────────────────────────────────────────────────

    public static class ManifestEntry {
        public final Manifest manifest;
        public final String version;
        public final Instant loadedAt;
        public final String rawJson;

        ManifestEntry(Manifest manifest, String version, Instant loadedAt, String rawJson) {
            this.manifest = manifest;
            this.version = version;
            this.loadedAt = loadedAt;
            this.rawJson = rawJson;
        }
    }

    public static class AuditEvent {
        public final Instant timestamp;
        public final String action; // ACCEPT, UPDATE, REJECT
        public final String deviceType;
        public final String details;

        AuditEvent(Instant timestamp, String action, String deviceType, String details) {
            this.timestamp = timestamp;
            this.action = action;
            this.deviceType = deviceType;
            this.details = details;
        }

        @Override
        public String toString() {
            return String.format("[%s] %s %s: %s", timestamp, action, deviceType, details);
        }
    }

    public static class ManifestStoreResult {
        private final boolean success;
        private final String message;
        private final String version;
        private final Manifest manifest;

        private ManifestStoreResult(boolean success, String message, String version, Manifest manifest) {
            this.success = success;
            this.message = message;
            this.version = version;
            this.manifest = manifest;
        }

        public static ManifestStoreResult success(String version, Manifest manifest) {
            return new ManifestStoreResult(true, null, version, manifest);
        }

        public static ManifestStoreResult failure(String message) {
            return new ManifestStoreResult(false, message, null, null);
        }

        public boolean success() {
            return success;
        }

        public String message() {
            return message;
        }

        public String version() {
            if (!success) {
                throw new IllegalStateException("Cannot get version from failed result");
            }
            return version;
        }

        public Manifest manifest() {
            if (!success) {
                throw new IllegalStateException("Cannot get manifest from failed result");
            }
            return manifest;
        }
    }
}
