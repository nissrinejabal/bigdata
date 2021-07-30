package com.jabal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CommandLineApp {
    private static final String WORKSPACE_PATH = "/mohammedamineboutouil/weather/data";

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        var conf = new Configuration();
        conf.set("yarn.resourcemanager.address", "localhost:8032");
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        var fs = FileSystem.get(conf);
        var fileStatusListIterator = fs.listFiles(
                new Path(WORKSPACE_PATH), true);

        while (fileStatusListIterator.hasNext()) {
            var fileStatus = fileStatusListIterator.next();

            // TODO: cli command hdfs dfs -ls /nissrinejabal/weather/data
            extractFilePath(fileStatus.getPath(), fs)
                    .forEach(s -> System.out.println("FILE: " + s.getName() + " ===> PATH: " + s));

            // TODO: cli command hdfs fsck /nissrinejabal/weather/data -files -blocks -locations
            Arrays.asList(fileStatus.getBlockLocations())
                    .forEach(s -> System.out.println("BLOCK: " + s.toString()));
        }
    }

    public static List<Path> extractFilePath(Path filePath, FileSystem fs) throws IOException {
        var returnedValue = new ArrayList<Path>();
        Arrays.stream(fs.listStatus(filePath))
                .forEachOrdered(fileStatus -> {
                    try {
                        if (fileStatus.isDirectory())
                            returnedValue.addAll(extractFilePath(fileStatus.getPath(), fs));
                        else
                            returnedValue.add(fileStatus.getPath());
                    } catch (IOException e) {
                        System.out.println("ERROR ACCORD :::: " + e.getMessage());
                    }
                });
        return returnedValue;
    }
}

