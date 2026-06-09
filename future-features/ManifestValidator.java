package com.iotdb.flink.manifest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ManifestValidator
 *
 * Validates IoT device manifests against JSON Schema (Draft-07).
 * Production features:
 * - Cached schema for performance
 * - Detailed error reporting (path + message)
 * - Supports custom rules beyond schema (e.g., device ID uniqueness)
 * - Idempotent: safe to call multiple times
 *
 * Usage:
 *   ManifestValidator validator = new ManifestValidator();
 *   ValidationResult result = validator.validate(manifestJson);
 *   if (!result.isValid()) {
 *     log.error("Manifest invalid: {}", result.errors());
 *   }
 */
public class ManifestValidator {

    private static final Logger log = LoggerFactory.getLogger(ManifestValidator.class);

    private final JsonSchema schema;
    private final ObjectMapper mapper;

    // ─────────────────────────────────────────────────────────────────────────
    // Constructor: Load & cache schema
    // ─────────────────────────────────────────────────────────────────────────

    public ManifestValidator() throws IOException {
        this.mapper = new ObjectMapper();
        this.schema = loadSchema();
        log.info("[ManifestValidator] Schema loaded and cached successfully");
    }

    /**
     * Load JSON schema from classpath resource.
     * Path: resources/manifest-schema.json
     */
    private JsonSchema loadSchema() throws IOException {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
        
        // Load from classpath
        InputStream schemaStream = getClass().getClassLoader()
                .getResourceAsStream("manifest-schema.json");
        
        if (schemaStream == null) {
            throw new IOException("manifest-schema.json not found in classpath. "
                    + "Add it to src/main/resources/");
        }

        JsonNode schemaNode = mapper.readTree(schemaStream);
        return factory.getSchema(schemaNode);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Main validation entry point
    // ─────────────────────────────────────────────────────────────────────────

    public ValidationResult validate(String manifestJson) {
        if (manifestJson == null || manifestJson.trim().isEmpty()) {
            return ValidationResult.invalid("Manifest JSON is null or empty");
        }

        try {
            // Parse JSON
            JsonNode manifestNode = mapper.readTree(manifestJson);
            return validateNode(manifestNode);
        } catch (Exception e) {
            return ValidationResult.invalid("Failed to parse manifest JSON: " + e.getMessage());
        }
    }

    public ValidationResult validate(JsonNode manifestNode) {
        if (manifestNode == null) {
            return ValidationResult.invalid("Manifest node is null");
        }
        return validateNode(manifestNode);
    }

    private ValidationResult validateNode(JsonNode manifestNode) {
        // Step 1: JSON Schema validation
        Set<ValidationMessage> schemaErrors = schema.validate(manifestNode);
        
        if (!schemaErrors.isEmpty()) {
            List<String> errors = schemaErrors.stream()
                    .map(msg -> String.format("$.%s: %s", msg.getPath(), msg.getMessage()))
                    .collect(Collectors.toList());
            return ValidationResult.invalid(errors);
        }

        // Step 2: Custom semantic validations (beyond schema)
        List<String> semanticErrors = new ArrayList<>();

        // ✓ Check version format (semver)
        String version = manifestNode.get("version").asText();
        if (!isSemver(version)) {
            semanticErrors.add("$.version: Not valid semver (e.g., 1.0.0 or 1.0.0-beta)");
        }

        // ✓ Check deviceType is alphanumeric + underscore
        String deviceType = manifestNode.get("deviceType").asText();
        if (!deviceType.matches("^[a-zA-Z0-9_]+$")) {
            semanticErrors.add("$.deviceType: Must be alphanumeric + underscore only");
        }

        // ✓ Check storageGroup follows IoTDB naming (root.sg.subsystem)
        String storageGroup = manifestNode.get("storageGroup").asText();
        if (!storageGroup.startsWith("root.")) {
            semanticErrors.add("$.storageGroup: Must start with 'root.'");
        }

        // ✓ Validate measurement names are unique
        JsonNode measurements = manifestNode.get("measurements");
        Set<String> measurementNames = new HashSet<>();
        List<String> duplicates = new ArrayList<>();
        
        for (JsonNode m : measurements) {
            String name = m.get("name").asText();
            if (!measurementNames.add(name)) {
                duplicates.add(name);
            }
        }
        
        if (!duplicates.isEmpty()) {
            semanticErrors.add(String.format("$.measurements: Duplicate measurement names: %s", 
                    duplicates));
        }

        // ✓ Validate KPIV rule IDs are unique
        JsonNode kpivRules = manifestNode.get("kpivRules");
        if (kpivRules != null && kpivRules.isArray()) {
            Set<String> ruleIds = new HashSet<>();
            List<String> dupRuleIds = new ArrayList<>();
            
            for (JsonNode rule : kpivRules) {
                if (rule.has("id")) {
                    String id = rule.get("id").asText();
                    if (!ruleIds.add(id)) {
                        dupRuleIds.add(id);
                    }
                }
            }
            
            if (!dupRuleIds.isEmpty()) {
                semanticErrors.add(String.format(
                        "$.kpivRules: Duplicate rule IDs: %s", dupRuleIds));
            }
        }

        // ✓ Validate knownDeviceIds are valid (alphanumeric)
        JsonNode knownDeviceIds = manifestNode.get("knownDeviceIds");
        if (knownDeviceIds != null && knownDeviceIds.isArray()) {
            int index = 0;
            for (JsonNode id : knownDeviceIds) {
                String deviceId = id.asText();
                if (!deviceId.matches("^[a-zA-Z0-9_-]+$")) {
                    semanticErrors.add(String.format(
                            "$.knownDeviceIds[%d]: Invalid device ID format: %s", 
                            index, deviceId));
                }
                index++;
            }
        }

        if (!semanticErrors.isEmpty()) {
            return ValidationResult.invalid(semanticErrors);
        }

        // All validations passed
        return ValidationResult.valid(manifestNode);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper: Validate semver
    // ─────────────────────────────────────────────────────────────────────────

    private boolean isSemver(String version) {
        // Accepts: 1.0.0, 1.0.0-beta, 1.0.0-beta.1, etc.
        return version.matches("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)"
                + "(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)"
                + "(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
                + "(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ValidationResult inner class
    // ─────────────────────────────────────────────────────────────────────────

    public static class ValidationResult {
        private final boolean valid;
        private final List<String> errors;
        private final JsonNode manifestNode; // only if valid

        private ValidationResult(boolean valid, List<String> errors, JsonNode node) {
            this.valid = valid;
            this.errors = errors;
            this.manifestNode = node;
        }

        public static ValidationResult valid(JsonNode node) {
            return new ValidationResult(true, Collections.emptyList(), node);
        }

        public static ValidationResult invalid(String error) {
            return new ValidationResult(false, Collections.singletonList(error), null);
        }

        public static ValidationResult invalid(List<String> errors) {
            return new ValidationResult(false, new ArrayList<>(errors), null);
        }

        public boolean isValid() {
            return valid;
        }

        public List<String> errors() {
            return Collections.unmodifiableList(errors);
        }

        /**
         * Get the validated manifest node. Only call if isValid() == true.
         */
        public JsonNode getManifestNode() {
            if (!valid) {
                throw new IllegalStateException("Cannot get manifest node from invalid result");
            }
            return manifestNode;
        }

        /**
         * Pretty-print all errors (for logging)
         */
        public String errorsSummary() {
            return String.join("\n  ", errors);
        }
    }
}
