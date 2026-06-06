package com.iotdb.flink.manifest;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Manifest
 *
 * Immutable representation of an IoT device-type manifest.
 * Used for schema definition, KPIV rules, and runtime configuration.
 *
 * Structure:
 * {
 *   "version": "1.0.0",
 *   "deviceType": "weather_sensor_v1",
 *   "storageGroup": "root.sg.weather",
 *   "devicePathPattern": "root.sg.weather.${deviceId}",
 *   "templateName": "tpl_weather_v1",
 *   "measurements": [...],
 *   "defaults": {...},
 *   "edgeTrimming": {...},
 *   "kpivRules": [...],
 *   "knownDeviceIds": [...],
 *   "meta": {...}
 * }
 */
public class Manifest implements Serializable {
    private static final long serialVersionUID = 1L;

    public String version;
    public String deviceType;
    public String storageGroup;
    public String devicePathPattern;
    public String templateName;
    public List<Measurement> measurements;
    public Defaults defaults;
    public EdgeTrimming edgeTrimming;
    public List<KPIVRule> kpivRules;
    public List<String> knownDeviceIds;
    public Map<String, Object> meta;

    // ─────────────────────────────────────────────────────────────────────────
    // Measurement
    // ─────────────────────────────────────────────────────────────────────────

    public static class Measurement implements Serializable {
        private static final long serialVersionUID = 1L;

        public String name;
        public String dataType; // BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT
        public String encoding;
        public String compression;
        public String unit;
        public String description;

        @Override
        public String toString() {
            return String.format("%s (%s)", name, dataType);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Defaults
    // ─────────────────────────────────────────────────────────────────────────

    public static class Defaults implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("timestampField")
        public String timestampField;

        @JsonProperty("deviceIdField")
        public String deviceIdField;

        @JsonProperty("timestampUnit")
        public String timestampUnit; // ms, s, us

        public Defaults() {
            // Fallback defaults if not provided
            this.timestampField = "ts";
            this.deviceIdField = "deviceId";
            this.timestampUnit = "ms";
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EdgeTrimming
    // ─────────────────────────────────────────────────────────────────────────

    public static class EdgeTrimming implements Serializable {
        private static final long serialVersionUID = 1L;

        public double leadSeconds;
        public double trailSeconds;
        public String action; // drop, tag

        public EdgeTrimming() {
            // Fallback defaults
            this.leadSeconds = 5.0;
            this.trailSeconds = 5.0;
            this.action = "drop";
        }

        public long getLeadMillis() {
            return (long) (leadSeconds * 1000);
        }

        public long getTrailMillis() {
            return (long) (trailSeconds * 1000);
        }

        @Override
        public String toString() {
            return String.format("EdgeTrimming(lead=%.1fs, trail=%.1fs, action=%s)",
                    leadSeconds, trailSeconds, action);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // KPIVRule
    // ─────────────────────────────────────────────────────────────────────────

    public static class KPIVRule implements Serializable {
        private static final long serialVersionUID = 1L;

        public String id;
        public String displayName;
        public String description;
        public String measurement;
        public String condition; // greater_than, less_than, between, outside, rate_above, rate_below, custom
        public Object threshold; // number or [number, number] for range
        public WindowSpec window;
        public double durationSec;
        public String severity; // info, warning, critical
        public List<String> actions;
        public String alertChannel;
        public Map<String, Object> params;

        @Override
        public String toString() {
            return String.format("Rule[%s: %s %s %s]", id, measurement, condition, threshold);
        }

        // Helper: is this rule a windowed aggregation?
        public boolean isWindowed() {
            return window != null && window.sizeSeconds > 0;
        }

        // Helper: get threshold as single number
        public double getThresholdAsDouble() {
            if (threshold instanceof Number) {
                return ((Number) threshold).doubleValue();
            }
            throw new IllegalArgumentException("Threshold is not a single number: " + threshold);
        }

        // Helper: get threshold as range [min, max]
        public double[] getThresholdAsRange() {
            if (threshold instanceof List) {
                List<?> list = (List<?>) threshold;
                if (list.size() == 2) {
                    return new double[]{
                            ((Number) list.get(0)).doubleValue(),
                            ((Number) list.get(1)).doubleValue()
                    };
                }
            }
            throw new IllegalArgumentException("Threshold is not a 2-element array: " + threshold);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // WindowSpec
    // ─────────────────────────────────────────────────────────────────────────

    public static class WindowSpec implements Serializable {
        private static final long serialVersionUID = 1L;

        public String type; // instant, tumbling, sliding
        public double sizeSeconds;
        public double slideSeconds;

        public long getSizeMillis() {
            return (long) (sizeSeconds * 1000);
        }

        public long getSlideMillis() {
            return (long) (slideSeconds * 1000);
        }

        @Override
        public String toString() {
            return String.format("Window[%s: size=%.1fs, slide=%.1fs]",
                    type, sizeSeconds, slideSeconds);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Convenience methods
    // ─────────────────────────────────────────────────────────────────────────

    public Measurement getMeasurement(String name) {
        if (measurements == null) {
            return null;
        }
        return measurements.stream()
                .filter(m -> m.name.equals(name))
                .findFirst()
                .orElse(null);
    }

    public KPIVRule getRule(String ruleId) {
        if (kpivRules == null) {
            return null;
        }
        return kpivRules.stream()
                .filter(r -> r.id.equals(ruleId))
                .findFirst()
                .orElse(null);
    }

    @Override
    public String toString() {
        return String.format(
                "Manifest[%s v%s | deviceType=%s | measurements=%d | rules=%d]",
                storageGroup, version, deviceType,
                measurements != null ? measurements.size() : 0,
                kpivRules != null ? kpivRules.size() : 0
        );
    }
}
