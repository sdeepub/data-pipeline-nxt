package com.iotdb.flink.manifest;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.List;

public class ManifestValidator {

    public ValidationResult validate(JsonNode node) {
        return new ValidationResult(true, Collections.emptyList());
    }

    public static class ValidationResult {
        private final boolean valid;
        private final List<String> errors;

        public ValidationResult(boolean valid, List<String> errors) {
            this.valid = valid;
            this.errors = errors;
        }

        public boolean isValid() {
            return valid;
        }

        public List<String> errors() {
            return errors;
        }

        public String errorsSummary() {
            return String.join(", ", errors);
        }
    }
}
