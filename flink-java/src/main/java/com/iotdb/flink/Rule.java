package com.iotdb.flink;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class Rule implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("rule_id")
    private String rule_id;

    @JsonProperty("version")
    private String version;

    @JsonProperty("device_type")
    private String device_type;

    @JsonProperty("rule_type")
    private String rule_type;

    @JsonProperty("measurement")
    private String measurement;

    @JsonProperty("condition")
    private String condition;

    @JsonProperty("threshold")
    private Double threshold;

    @JsonProperty("severity")
    private String severity;

    @JsonProperty("enabled")
    private Boolean enabled;

    // Getters & Setters
    public String getRule_id() { return rule_id; }
    public void setRule_id(String rule_id) { this.rule_id = rule_id; }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public String getDevice_type() { return device_type; }
    public void setDevice_type(String device_type) { this.device_type = device_type; }

    public String getRule_type() { return rule_type; }
    public void setRule_type(String rule_type) { this.rule_type = rule_type; }

    public String getMeasurement() { return measurement; }
    public void setMeasurement(String measurement) { this.measurement = measurement; }

    public String getCondition() { return condition; }
    public void setCondition(String condition) { this.condition = condition; }

    public Double getThreshold() { return threshold; }
    public void setThreshold(Double threshold) { this.threshold = threshold; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public boolean isEnabled() { return enabled != null ? enabled : true; }
    public void setEnabled(Boolean enabled) { this.enabled = enabled; }
}
