package com.iotdb.flink;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class ContextAnomaly implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("type")
    public String type;

    @JsonProperty("previous_run_id")
    public String previous_run_id;

    @JsonProperty("current_run_id")
    public String current_run_id;

    @JsonProperty("implicit_transition_ts")
    public Long implicit_transition_ts;

    @JsonProperty("reason")
    public String reason;
}
