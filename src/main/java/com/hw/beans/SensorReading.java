package com.hw.beans;

public class SensorReading {

    private String sensorId;

    private Long timestamp;

    private Double temp;

    public SensorReading() {
    }

    public SensorReading(String sensorId, Long timestamp, Double temp) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.temp = temp;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemp() {
        return temp;
    }

    public void setTemp(Double temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "sensorId='" + sensorId + '\'' +
                ", timestamp=" + timestamp +
                ", temp=" + temp +
                '}';
    }
}
