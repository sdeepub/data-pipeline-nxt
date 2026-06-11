package com.iotdb.flink;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class EvaluationResult implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("device_id")
    public String device_id;

    @JsonProperty("timestamp")
    public Long timestamp;

    @JsonProperty("gas_temperature")
    public Double gas_temperature;

    @JsonProperty("gas_pressure")
    public Double gas_pressure;

    @JsonProperty("humidity")
    public Double humidity;

    @JsonProperty("spin_rate")
    public Double spin_rate;

    @JsonProperty("torque")
    public Double torque;

    @JsonProperty("status")
    public String status;

    @JsonProperty("fault_code")
    public String fault_code;

    @JsonProperty("passed")
    public boolean passed;

    @JsonProperty("alert")
    public AlertDetails alert;

    @JsonProperty("context_anomaly")
    public ContextAnomaly context_anomaly;
}
