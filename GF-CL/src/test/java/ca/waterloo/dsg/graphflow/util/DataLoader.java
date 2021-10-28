package ca.waterloo.dsg.graphflow.util;

import ca.waterloo.dsg.graphflow.runner.utils.DatasetMetadata;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.loader.GraphLoader;
import ca.waterloo.dsg.graphflow.storage.loader.NodeIDMapping;
import ca.waterloo.dsg.graphflow.util.container.Pair;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Exposes functions for creation and deletion of the {@link Graph} and {@link DatasetMetadata}
 * from a given dataset folder to be used in Tests. Further contains methods to load vertex and
 * edge files for a dataset.
 * <p>
 * {@link DatasetMetadata} holds information about the characteristics of the dataset that are
 * useful while performing various tests. It is loaded from the {@code metadata.json} which is
 * located at the root directory of the dataset.
 */
public class DataLoader {

    public static class Dataset {

        public Graph graph;
        DatasetMetadata metadata;
        public NodeIDMapping nodeIDMapping;
        public Pair<DataType[], int[]>[] nodePropertyDescriptions, relPropertyDescriptions;
        private List<List<String[]>> nodeData;
    }

    private static final Logger logger = LogManager.getLogger(DataLoader.class);

    public static String RESOURCES_PATH = "src/test/resources/datasets";

    private static Map<String, Dataset> datasets;

    public static Dataset getDataset(String datasetName) {
        if (null == datasets || !datasets.containsKey(datasetName)) {
            loadDataset(datasetName);
        }
        return datasets.get(datasetName);
    }

    public static List<List<String[]>> getNodeData(String datasetName) throws IOException {
        var dataset = getDataset(datasetName);
        if (null == dataset.nodeData) {
            loadNodeData(datasetName, datasets.get(datasetName));
        }
        return datasets.get(datasetName).nodeData;
    }

    private static void loadDataset(String datasetName) {
        var absolutePath = new File(RESOURCES_PATH).getAbsolutePath() + "/" + datasetName + "/";
        if (null == datasets) {
            datasets = new HashMap<>();
        }
        try {
            var metadata = DatasetMetadata.readDatasetMetadata(absolutePath);
            var graphLoader = new GraphLoader();
            graphLoader.loadGraph(absolutePath, metadata, null);
            var dataset = new Dataset();
            dataset.graph = graphLoader.getGraph();
            dataset.metadata = metadata;
            dataset.nodeIDMapping = graphLoader.getNodeIDMapping();
            dataset.nodePropertyDescriptions = graphLoader.getNodePropertyDescriptions();
            dataset.relPropertyDescriptions = graphLoader.getRelPropertyDescriptions();
            datasets.put(datasetName, dataset);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error reading files: " + e.getMessage());
        }
    }

    private static void loadNodeData(String datasetName, Dataset dataset) throws IOException {
        var absolutePath = new File(RESOURCES_PATH).getAbsolutePath() + "/" + datasetName + "/";
        var data = new ArrayList<List<String[]>>();
        for (var i=0; i < dataset.nodePropertyDescriptions.length; i++) {
            data.add(null);
        }
        var catalog = dataset.graph.getGraphCatalog();
        var metadata = dataset.metadata;
        for (var vertexFile : metadata.nodeFileDescriptions) {
            var type = catalog.getTypeKey(vertexFile.getType());
            var reader = new BufferedReader(new FileReader(absolutePath + "/" +
                vertexFile.getFilename()));
            reader.readLine(); /* header */
            var vertexDataOfAType = new ArrayList<String[]>();
            var line = reader.readLine();
            while (null != line) {
                vertexDataOfAType.add(line.split(","));
                line = reader.readLine();
            }
            Collections.shuffle(vertexDataOfAType);
            data.set(type, vertexDataOfAType);
        }
        dataset.nodeData = data;
    }
}
