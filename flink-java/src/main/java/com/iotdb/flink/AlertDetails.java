package com.iotdb.flink;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class AlertDetails implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("alert_id")
    public String alert_id;

    @JsonProperty("alert_time")
    public Long alert_time;

    @JsonProperty("rule_id")
    public String rule_id;

    @JsonProperty("alert_type")
    public String alert_type;

    @JsonProperty("measurement")
    public String measurement;

    @JsonProperty("value")
    public Double value;

    @JsonProperty("threshold")
    public Double threshold;

    @JsonProperty("severity")
    public String severity;

    @JsonProperty("message")
    public String message;
}
