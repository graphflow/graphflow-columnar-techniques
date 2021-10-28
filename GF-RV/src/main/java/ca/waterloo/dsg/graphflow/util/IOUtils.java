package ca.waterloo.dsg.graphflow.util;

import ca.waterloo.dsg.graphflow.storage.graph.BucketOffsetManager;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore.UnstructuredNodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.graph.properties.relpropertystore.UnstructuredRelPropertyStore;
import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.AdjListIndexes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class IOUtils {

    private static final Logger logger = LogManager.getLogger(IOUtils.class);

    public static double getTimeDiff(long beginTime) {
        return (System.nanoTime() - beginTime) / 1000000.0;
    }

    public static void mkdirs(String directoryPath) throws IOException {
        var file = new File(directoryPath);
        if (!file.exists()) {
            if (!file.mkdirs()) {
                throw new IOException("The directory " + directoryPath + " was not created.");
            }
        }
    }

    public static Graph deserializeGraph(String directory) throws IOException,
        ClassNotFoundException, InterruptedException {
        long aTime = System.nanoTime();
        logger.info("Starting Graph deserialization...");
        var graph = new Graph();
        var sTime = System.nanoTime();
        graph.setNodePropertyStore(UnstructuredNodePropertyStore.deserialize(directory));
        logger.info(String.format("Deserialized `NodePropertyStore` in %.2f ms.",
            + getTimeDiff(sTime)));
        sTime = System.nanoTime();
        graph.setRelPropertyStore(UnstructuredRelPropertyStore.deserialize(directory));
        logger.info(String.format("Deserialized `RelPropertyStore` in %.2f ms.",
            + getTimeDiff(sTime)));
        sTime = System.nanoTime();
        graph.setBucketOffsetManagers((BucketOffsetManager[][]) deserializeObject(
            directory + "bucketOffsetManagers"));
        logger.info(String.format("Deserialized `BucketStoreManager[][]` in %.2f ms.", +
            getTimeDiff(sTime)));
        sTime = System.nanoTime();
        graph.setAdjListIndexes(AdjListIndexes.deserialize(directory));
        logger.info(String.format("Deserialized `AdjListIndexes` in %.2f ms.", +
            getTimeDiff(sTime)));
        graph.setNumNodes((long) deserializeObject(directory + "numNodes"));
        graph.setNumNodesPerType((long[]) deserializeObject(directory + "numNodesPerType"));
        graph.setNumRels((long) deserializeObject(directory + "numRels"));
        graph.setNumRelsPerLabel((long[]) deserializeObject(directory + "numRelsPerLabel"));
        graph.setGraphCatalog((GraphCatalog) deserializeObject(directory + "graphCatalog"));
        logger.info(String.format("Deserialized complete Graph in %.2f ms.", + getTimeDiff(aTime)));
        return graph;
    }

    public static Object deserializeObject(String file) throws IOException, ClassNotFoundException {
        var inputStream = new ObjectInputStream(new BufferedInputStream(
            new FileInputStream(file)));
        Object object = inputStream.readObject();
        inputStream.close();
        return object;
    }
}
