package com.jabal.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, WeatherWritable> {

    protected static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, WeatherWritable>.Context context)
            throws IOException, InterruptedException {
        // Convert the single row(Record) to String and store it in String variable name line
        var line = value.toString();
        // Check for the empty line
        if (!(line.length() == 0)) {
            var values = Stream.of(
                    line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
            )
                    .map(String::trim)
                    .map(s -> s.replace("\"", ""))
                    .collect(toList());

            long year,station,tempMeasure = 0;
            double tempNb = 0;
            // Check for the header line
            if (!values.get(0).equals("STATION")) {
                station = Long.parseLong(values.get(0));
                year = Long.parseLong(values.get(1).substring(0, 4));
                var temp = Double.parseDouble(values.get(13).substring(0, 5));
                var tempQuality = Integer.parseInt(String.valueOf(values.get(13).charAt(6)));

                if (temp != MISSING && tempQuality <= 4) {
                    tempMeasure = 1;
                    tempNb = temp / 10;
                }

                var outKey = values.get(6) + " - " + values.get(1).substring(0, 7);

                var outVal = new WeatherWritable(
                        year,
                        station, tempMeasure,
                        tempNb, tempNb, tempNb
                );

                if (tempMeasure != 0) {
                    context.write(new Text(outKey),
                            outVal
                    );
                }
            }
        }
    }
}