package ca.waterloo.dsg.graphflow.util;

import lombok.Getter;

import java.io.IOException;
import java.util.Properties;

public class Configuration {

    // maximum number of threads to use for computing tasks.
    @Getter private static int maxComputingThreads;

    // maximum number of threads to use for I/O intensive tasks.
    @Getter private static int maxIOThreads;

    // size of file chunk to be processed by one thread.
    @Getter private static int blockSize;

    // size of buffer pair for reading a line from the input files.
    @Getter private static int lineSize;

    // number of AdjLisGroups to be processed per thread.
    @Getter private static int numAdjListGroupsPerThread;

    // number of AdjLists to be grouped together.
    @Getter private static int defaultAdjListGroupingSize;

    // number of AdjListGroups per file.
    @Getter private static int numAdjListGroupsPerFile;

    static  {
        var properties = new Properties();
        try {
            properties.load(IOUtils.class.getClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        maxComputingThreads = Integer.parseInt(properties.getProperty(
            "maxComputingThreads"));
        maxIOThreads = Integer.parseInt(properties.getProperty("maxIOThreads"));
        blockSize = Integer.parseInt(properties.getProperty("blockSize"));
        lineSize = Integer.parseInt(properties.getProperty("lineSize"));
        numAdjListGroupsPerThread = Integer.parseInt(properties.getProperty(
            "numAdjListGroupsPerThread"));
        defaultAdjListGroupingSize = Integer.parseInt(properties.getProperty(
            "adjListGroupingSize"));
        numAdjListGroupsPerFile = Integer.parseInt(properties.getProperty(
            "numAdjListGroupsPerFile"));
    }
}
