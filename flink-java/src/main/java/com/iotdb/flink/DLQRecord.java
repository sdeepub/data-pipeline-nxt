package com.iotdb.flink;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class DLQRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("anomaly_id")
    public String anomaly_id;

    @JsonProperty("device_id")
    public String device_id;

    @JsonProperty("anomaly_type")
    public String anomaly_type;

    @JsonProperty("context_anomaly")
    public ContextAnomaly context_anomaly;

    @JsonProperty("status")
    public String status;

    @JsonProperty("resolution_action")
    public String resolution_action;

    @JsonProperty("created_ts")
    public Long created_ts;

    @JsonProperty("resolved_ts")
    public Long resolved_ts;
}
