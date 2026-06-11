package com.iotdb.flink;

import java.io.Serializable;

public class ContextAwareTelemetry implements Serializable {

    private SensorData telemetry;

    private String device_id;
    private String run_id;
    private String event_type;

    private Long start_ts;
    private Long end_ts;

    public ContextAwareTelemetry() {
    }

    public SensorData getTelemetry() {
        return telemetry;
    }

    public void setTelemetry(SensorData telemetry) {
        this.telemetry = telemetry;
    }

    public String getDeviceId() {
        return device_id;
    }

    public void setDeviceId(String device_id) {
        this.device_id = device_id;
    }

    public String getRunId() {
        return run_id;
    }

    public void setRunId(String run_id) {
        this.run_id = run_id;
    }

    public String getEventType() {
        return event_type;
    }

    public void setEventType(String event_type) {
        this.event_type = event_type;
    }

    public Long getStartTs() {
        return start_ts;
    }

    public void setStartTs(Long start_ts) {
        this.start_ts = start_ts;
    }

    public Long getEndTs() {
        return end_ts;
    }

    public void setEndTs(Long end_ts) {
        this.end_ts = end_ts;
    }
}
