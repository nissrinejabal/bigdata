package com.jabal.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeatherWritable implements Writable {

    private long year;
    private long station;
    private long tempMeasure;

    private double minTemp;
    private double maxTemp;
    private double avgTemp;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(year);
        dataOutput.writeLong(station);
        dataOutput.writeLong(tempMeasure);
        dataOutput.writeDouble(minTemp);
        dataOutput.writeDouble(maxTemp);
        dataOutput.writeDouble(avgTemp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readLong();
        this.station = dataInput.readLong();
        this.tempMeasure = dataInput.readLong();
        this.minTemp = dataInput.readDouble();
        this.maxTemp = dataInput.readDouble();
        this.avgTemp = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "WeatherWritable {" +
                "Station = " + station +
                ", Year = " + year +
                ", Temp Measure = " + tempMeasure +
                ", Min Temp = " + minTemp + "℃" +
                ", Max Temp = " + maxTemp + "℃" +
                ", Avg Temp = " + avgTemp + "℃" +
                '}';
    }

    public WeatherWritable() {
        super();
    }

    public WeatherWritable(long tempMeasure,
                           double minTemp, double maxTemp, double avgTemp
    ) {
        this.tempMeasure = tempMeasure;
        this.minTemp = minTemp;
        this.maxTemp = maxTemp;
        this.avgTemp = avgTemp;
    }

    public WeatherWritable(
            long year,
            long station, long tempMeasure,
            double minTemp, double maxTemp, double avgTemp
    ) {
        this.year = year;
        this.station = station;
        this.tempMeasure = tempMeasure;
        this.minTemp = minTemp;
        this.maxTemp = maxTemp;
        this.avgTemp = avgTemp;
    }

    public long getYear() {
        return year;
    }

    public void setYear(long year) {
        this.year = year;
    }

    public long getStation() {
        return station;
    }

    public void setStation(long station) {
        this.station = station;
    }

    public long getTempMeasure() {
        return tempMeasure;
    }

    public void setTempMeasure(long tempMeasure) {
        this.tempMeasure = tempMeasure;
    }

    public double getMinTemp() {
        return minTemp;
    }

    public void setMinTemp(double minTemp) {
        this.minTemp = minTemp;
    }

    public double getMaxTemp() {
        return maxTemp;
    }

    public void setMaxTemp(double maxTemp) {
        this.maxTemp = maxTemp;
    }

    public double getAvgTemp() {
        return avgTemp;
    }

    public void setAvgTemp(double avgTemp) {
        this.avgTemp = avgTemp;
    }
}