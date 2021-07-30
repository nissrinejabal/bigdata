package com.jabal;

import com.boutouil.mapreduce.WeatherMapper;
import com.boutouil.mapreduce.WeatherReducer;
import com.boutouil.mapreduce.WeatherWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class WeatherDataApp {

    private static final String WORKSPACE_PATH = "/nissrinejabal/weather/data";

    /**
     * @method main
     * This method is used for setting all the configuration properties.
     * It acts as a driver for map-reduce code.
     */
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        //reads the default configuration of the cluster from the configuration XML files
        //Initializing the job with the default configuration of the cluster
        var conf = new Configuration();
        conf.set("yarn.resourcemanager.address", "localhost:8032");
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        var job = Job.getInstance(conf, "Weather data analysis");

        var fs = FileSystem.get(conf);
        var fileStatusListIterator = fs.listFiles(
                new Path(WORKSPACE_PATH), true);
        while (fileStatusListIterator.hasNext()) {
            var fileStatus = fileStatusListIterator.next();
            var paths = CommandLineApp.extractFilePath(fileStatus.getPath(), fs);
            for (Path in : paths) {
                FileInputFormat.addInputPath(job, in);
            }
        }

        var out = new Path(WORKSPACE_PATH + "/weather_output");
        FileOutputFormat.setOutputPath(job, out);

        //deleting the context path automatically from hdfs so that we don't have to delete it explicitly
        out.getFileSystem(job.getConfiguration())
                .delete(out, true);

        job.setJarByClass(WeatherDataApp.class);
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WeatherWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //exiting the job only if the flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}