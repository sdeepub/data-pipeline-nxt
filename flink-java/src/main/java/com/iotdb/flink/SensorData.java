package com.iotdb.flink;

public class SensorData {

    private String machine_id;
    private String machine_type;
    private String location;

    private long timestamp;

    private double gas_temperature;
    private double gas_pressure;
    private double humidity;
    private double spin_rate;
    private double torque;

    private String status;
    private String fault_code;

    // ------------------------------------------------------------
    // Default Constructor (Required for Jackson JSON Parsing)
    // ------------------------------------------------------------

    public SensorData() {
    }

    // ------------------------------------------------------------
    // Getters and Setters
    // ------------------------------------------------------------

    public String getMachine_id() {
        return machine_id;
    }

    public void setMachine_id(String machine_id) {
        this.machine_id = machine_id;
    }

    public String getMachine_type() {
        return machine_type;
    }

    public void setMachine_type(String machine_type) {
        this.machine_type = machine_type;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getGas_temperature() {
        return gas_temperature;
    }

    public void setGas_temperature(double gas_temperature) {
        this.gas_temperature = gas_temperature;
    }

    public double getGas_pressure() {
        return gas_pressure;
    }

    public void setGas_pressure(double gas_pressure) {
        this.gas_pressure = gas_pressure;
    }

    public double getHumidity() {
        return humidity;
    }

    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

    public double getSpin_rate() {
        return spin_rate;
    }

    public void setSpin_rate(double spin_rate) {
        this.spin_rate = spin_rate;
    }

    public double getTorque() {
        return torque;
    }

    public void setTorque(double torque) {
        this.torque = torque;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getFault_code() {
        return fault_code;
    }

    public void setFault_code(String fault_code) {
        this.fault_code = fault_code;
    }

    // ------------------------------------------------------------
    // toString()
    // ------------------------------------------------------------

    @Override
    public String toString() {
        return "SensorData{" +
                "machine_id='" + machine_id + '\'' +
                ", machine_type='" + machine_type + '\'' +
                ", location='" + location + '\'' +
                ", timestamp=" + timestamp +
                ", gas_temperature=" + gas_temperature +
                ", gas_pressure=" + gas_pressure +
                ", humidity=" + humidity +
                ", spin_rate=" + spin_rate +
                ", torque=" + torque +
                ", status='" + status + '\'' +
                ", fault_code='" + fault_code + '\'' +
                '}';
    }
}
