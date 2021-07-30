package com.jabal.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<Text, WeatherWritable, Text, WeatherWritable> {
    @Override
    protected void reduce(Text key, Iterable<WeatherWritable> values,
                          Reducer<Text, WeatherWritable, Text, WeatherWritable>.Context context
    ) throws IOException, InterruptedException {
        long tempMeasure = 0;
        double minTemp = 0;
        double maxTemp = 0;
        double sumTemp = 0;

        for (WeatherWritable val : values) {
            if (val.getTempMeasure() != 0) {
                sumTemp += val.getAvgTemp();
                tempMeasure += val.getTempMeasure();
                if (maxTemp < val.getMaxTemp())
                    maxTemp = val.getMaxTemp();
                if (minTemp > val.getMinTemp())
                    minTemp = val.getMinTemp();
            }

            sumTemp = sumTemp / tempMeasure;

            var outVal = new WeatherWritable(
                    tempMeasure,
                    minTemp, maxTemp, sumTemp
            );

            if (tempMeasure != 0) {
                context.write(key, outVal);
            }
        }
    }
}
