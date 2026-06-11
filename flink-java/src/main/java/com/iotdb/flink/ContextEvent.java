package com.iotdb.flink;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class ContextEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("device_id")
    private String device_id;

    @JsonProperty("run_id")
    private String run_id;

    @JsonProperty("event_type")
    private String event_type;

    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("start_ts")
    private Long start_ts;

    @JsonProperty("end_ts")
    private Long end_ts;

    // Getters & Setters
    public String getDevice_id() { return device_id; }
    public void setDevice_id(String device_id) { this.device_id = device_id; }

    public String getRun_id() { return run_id; }
    public void setRun_id(String run_id) { this.run_id = run_id; }

    public String getEvent_type() { return event_type; }
    public void setEvent_type(String event_type) { this.event_type = event_type; }

    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

    public Long getStart_ts() { return start_ts; }
    public void setStart_ts(Long start_ts) { this.start_ts = start_ts; }

    public Long getEnd_ts() { return end_ts; }
    public void setEnd_ts(Long end_ts) { this.end_ts = end_ts; }
}
