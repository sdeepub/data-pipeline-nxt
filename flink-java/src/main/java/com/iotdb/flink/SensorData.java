package com.iotdb.flink;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class SensorData implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("machine_id")
    private String machine_id;

    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("gas_temperature")
    private Double gas_temperature;

    @JsonProperty("gas_pressure")
    private Double gas_pressure;

    @JsonProperty("humidity")
    private Double humidity;

    @JsonProperty("spin_rate")
    private Double spin_rate;

    @JsonProperty("torque")
    private Double torque;

    @JsonProperty("status")
    private String status;

    @JsonProperty("fault_code")
    private String fault_code;

    // Getters & Setters
    public String getMachine_id() { return machine_id; }
    public void setMachine_id(String machine_id) { this.machine_id = machine_id; }

    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

    public Double getGas_temperature() { return gas_temperature; }
    public void setGas_temperature(Double gas_temperature) { this.gas_temperature = gas_temperature; }

    public Double getGas_pressure() { return gas_pressure; }
    public void setGas_pressure(Double gas_pressure) { this.gas_pressure = gas_pressure; }

    public Double getHumidity() { return humidity; }
    public void setHumidity(Double humidity) { this.humidity = humidity; }

    public Double getSpin_rate() { return spin_rate; }
    public void setSpin_rate(Double spin_rate) { this.spin_rate = spin_rate; }

    public Double getTorque() { return torque; }
    public void setTorque(Double torque) { this.torque = torque; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getFault_code() { return fault_code; }
    public void setFault_code(String fault_code) { this.fault_code = fault_code; }
}
