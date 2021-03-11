package ca.waterloo.dsg.graphflow.storage.loader;

import ca.waterloo.dsg.graphflow.runner.utils.DatasetMetadata;
import ca.waterloo.dsg.graphflow.storage.BucketOffsetManager;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.AdjListIndexes;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex.ColumnAdjListIndex;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex.ColumnAdjListIndexMultiType;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex.ColumnAdjListIndexSingleType;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.DefaultAdjListIndex;
import ca.waterloo.dsg.graphflow.storage.loader.threads.AdjListSerializerThread;
import ca.waterloo.dsg.graphflow.storage.loader.threads.AdjListSerializerThread.AdjListSerializerTaskManager;
import ca.waterloo.dsg.graphflow.storage.loader.threads.AdjacencyListPopulatorThread;
import ca.waterloo.dsg.graphflow.storage.loader.threads.BucketOffsetsEnumeratorThread;
import ca.waterloo.dsg.graphflow.storage.loader.threads.BucketOffsetsEnumeratorThread.EnumerationType;
import ca.waterloo.dsg.graphflow.storage.loader.threads.FileBlockLinesCounterThread;
import ca.waterloo.dsg.graphflow.storage.loader.threads.NodePropertiesReaderThread;
import ca.waterloo.dsg.graphflow.storage.loader.threads.RelLabelPerNodeProfilerThread;
import ca.waterloo.dsg.graphflow.storage.loader.threads.RelPropertiesReaderThread;
import ca.waterloo.dsg.graphflow.storage.loader.threads.RelsReaderThread;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.NodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.RelPropertyStore;
import ca.waterloo.dsg.graphflow.util.Configuration;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import ca.waterloo.dsg.graphflow.util.container.Pair;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Constructs a {@link Graph} object from CSV file and binary serialized blocks.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class GraphLoader {

    private static final Logger logger = LogManager.getLogger(GraphLoader.class);

    @Getter private NodeIDMapping nodeIDMapping;
    @Getter private Pair<DataType[], int[]>[] nodePropertyDescriptions, relPropertyDescriptions;
    private String[] nodeFiles, relFiles;
    private char separator;
    private String outputDirectory;

    private long numNodes = 0L, numRels = 0L;
    private long[] numNodesPerType, numRelsPerLabel;
    @Getter private Graph graph = new Graph();
    private GraphCatalog catalog = new GraphCatalog();
    private AdjListIndexes adjListIndexes = new AdjListIndexes();

    public void loadGraph(String inputDirectory, DatasetMetadata metadata, String outputDirectory)
        throws InterruptedException, IOException {
        this.separator = metadata.separator;
        this.outputDirectory = outputDirectory;
        logMemory();
        var aTime = System.nanoTime();
        catalog.init(metadata);
        loadFileList(inputDirectory, metadata);
        loadNodes();
        loadRels();
        if (null != outputDirectory) {
            serializeObject("numNodes", numNodes);
            serializeObject("numNodesPerType", numNodesPerType);
            serializeObject("numRels", numRels);
            serializeObject("numRelsPerLabel", numRelsPerLabel);
            serializeObject("graphCatalog", catalog);
        } else {
            graph.setNumNodes(numNodes);
            graph.setNumNodesPerType(numNodesPerType);
            graph.setNumRels(numRels);
            graph.setNumRelsPerLabel(numRelsPerLabel);
            graph.setGraphCatalog(catalog);
        }
        logger.info(String.format("Completed Graph Loading in %.2f ms", IOUtils.getTimeDiff(aTime)));
    }

    private void loadFileList(String inputDirectory, DatasetMetadata metadata) {
        this.nodeFiles = new String[catalog.getNumTypes()];
        for (var nodeFile: metadata.nodeFileDescriptions) {
            var type = catalog.getTypeKey(nodeFile.getType());
            nodeFiles[type] = inputDirectory + "/" + nodeFile.getFilename();
        }
        this.relFiles = new String[catalog.getNumLabels()];
        for (var edgeFile: metadata.relFileDescriptions) {
            var label = catalog.getLabelKey(edgeFile.getLabel());
            this.relFiles[label] = inputDirectory + "/" + edgeFile.getFilename();
        }
    }

    private void loadNodes() throws InterruptedException, IOException {
        readNodeFileHeaders();
        var numLinesPerBlock = new long[catalog.getNumTypes()][];
        countNodes(numLinesPerBlock);
        numNodesPerType = new long[catalog.getNumTypes()];
        for (var type = 0; type < catalog.getNumTypes(); type++) {
            numLinesPerBlock[type][0]--;
            numNodesPerType[type] = Arrays.stream(numLinesPerBlock[type]).reduce(0L, Long::sum);
        }
        numNodes = Arrays.stream(numNodesPerType).reduce(0, Long::sum);
        logger.info(String.format("numNodes per type %s, total = %d",
            Arrays.toString(numNodesPerType), numNodes));
        logger.info("Initializing `nodeIdMap`, `nodePropertyStore`.");
        logMemory();
        nodeIDMapping = new NodeIDMapping(numNodes);
        var nodePropertyStore = new NodePropertyStore();
        nodePropertyStore.init(nodePropertyDescriptions, numNodesPerType);
        readNodeProperties(nodePropertyStore, numLinesPerBlock);
        if (null != outputDirectory) {
            var aTime = System.nanoTime();
            logger.info("Initiating [nodeStoreSerialization]");
            logMemory();
            nodePropertyStore.serialize(outputDirectory);
            logger.info(String.format("Exiting [nodeStoreSerialization], completed in %.2f ms",
                IOUtils.getTimeDiff(aTime)));
            logMemory();
        } else {
            graph.setNodePropertyStore(nodePropertyStore);
        }
    }

    private void readNodeFileHeaders()
        throws IOException {
        var aTime = System.nanoTime();
        logger.info("Initiating [readNodeFileHeaders]");
        logMemory();
        nodePropertyDescriptions = new Pair[catalog.getNumTypes()];
        for (var type = 0; type < catalog.getNumTypes(); type++) {
            var reader = new BufferedReader(new FileReader(nodeFiles[type]));
            var propertiesArr = reader.readLine().split(String.valueOf(separator));
            reader.close();
            var indexToPropertyKey = new int[propertiesArr.length];
            var dataTypes = new DataType[propertiesArr.length];
            for (int propertyIdx = 1 ; propertyIdx < propertiesArr.length; propertyIdx++) {
                var propertyDataType = propertiesArr[propertyIdx].split(":" /*COLON*/);
                var dataType = DataType.getDataType(propertyDataType[1]);
                dataTypes[propertyIdx] = dataType;
                indexToPropertyKey[propertyIdx] = catalog.insertNodePropertyKeyIfNeeded(
                    propertyDataType[0], dataType);
            }
            nodePropertyDescriptions[type] = new Pair<>(dataTypes, indexToPropertyKey);
        }
        logger.info(String.format("Exiting [readNodeFileHeaders], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    private void countNodes(long[][] numLinesPerBlock) throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [countNodes]");
        logMemory();
        var numBlocks = new int[catalog.getNumTypes()];
        for (var type = 0; type < catalog.getNumTypes(); type++) {
            var numBlocksOfAType = 1 + (int) (new File(nodeFiles[type]).length() /
                Configuration.getBlockSize());
            numBlocks[type] = numBlocksOfAType;
            numLinesPerBlock[type] = new long[numBlocksOfAType];
        }
        var taskManager = new FileBlockLinesCounterThread.FileBlockLinesCounterTaskManager(
            nodeFiles, numBlocks);
        var numThread = Configuration.getMaxIOThreads();
        var executor = Executors.newFixedThreadPool(numThread);
        for (var i = 0; i < numThread; i++) {
            executor.execute(new FileBlockLinesCounterThread(separator, numLinesPerBlock, taskManager));
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        logger.info(String.format("Exiting [countNodes], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    private void readNodeProperties(NodePropertyStore nodePropertyStore, long[][] numLinesPerBlock)
        throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [readNodeProperties]");
        logMemory();
        var taskManager = new FileBlockLinesCounterThread.FileBlockLinesCounterTaskManager(
            nodeFiles, Arrays.stream(numLinesPerBlock).mapToInt(blocks -> blocks.length).toArray());
        var numThread = Configuration.getMaxComputingThreads();
        var executor = Executors.newFixedThreadPool(numThread);
        for (var i = 0; i < numThread; i++) {
            executor.execute(new NodePropertiesReaderThread(separator, numLinesPerBlock,
                nodeIDMapping, nodePropertyStore, nodePropertyDescriptions, taskManager));
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        logger.info(String.format("Exiting [readNodeProperties], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    private void loadRels() throws InterruptedException, IOException {
        readRelFileHeaders();
        var numLinesPerBlock = new long[catalog.getNumLabels()][];
        countRels(numLinesPerBlock);
        numRelsPerLabel = new long[catalog.getNumLabels()];
        for (var label = 0; label < catalog.getNumLabels(); label++) {
            numLinesPerBlock[label][0]--;
            numRelsPerLabel[label] = Arrays.stream(numLinesPerBlock[label]).reduce(0L, Long::sum);
        }
        numRels = Arrays.stream(numRelsPerLabel).reduce(0, Long::sum);
        logger.info(String.format("NumRels per label %s, total = %d", Arrays.toString(
            numRelsPerLabel), numRels));
        var edgesNbrTypesAndBucketOffsets = new int[catalog.getNumLabels()][][];
        var edgesNbrOffsets = new long[catalog.getNumLabels()][][];
        readRels(edgesNbrTypesAndBucketOffsets, edgesNbrOffsets, numLinesPerBlock);
        fillGraphCatalog(edgesNbrTypesAndBucketOffsets);
        var bucketOffsetManagers = initBucketOffsetManagers(edgesNbrTypesAndBucketOffsets,
            edgesNbrOffsets);
        initRelStores(bucketOffsetManagers, numLinesPerBlock, edgesNbrTypesAndBucketOffsets,
            edgesNbrOffsets);
        createAdjListsIndex(edgesNbrTypesAndBucketOffsets, edgesNbrOffsets, Direction.FORWARD);
        createAdjListsIndex(edgesNbrTypesAndBucketOffsets, edgesNbrOffsets, Direction.BACKWARD);
        graph.setAdjListIndexes(adjListIndexes);
    }

    private void readRelFileHeaders() throws IOException {
        var aTime = System.nanoTime();
        logger.info("Initiating [readRelFileHeaders]");
        logMemory();
        relPropertyDescriptions = new Pair[catalog.getNumLabels()];
        for (var label = 0; label < catalog.getNumLabels(); label++) {
            var reader = new BufferedReader(new FileReader(relFiles[label]));
            var propertiesArr = reader.readLine().split(String.valueOf(separator));
            reader.close();
            var indexToPropertyKey = new int[propertiesArr.length];
            var dataTypes = new DataType[propertiesArr.length];
            for (int propertyIdx = 2 ; propertyIdx < propertiesArr.length; propertyIdx++) {
                var propertyDataType = propertiesArr[propertyIdx].split(":" /*COLON*/);
                var dataType = DataType.getDataType(propertyDataType[1]);
                dataTypes[propertyIdx] = dataType;
                indexToPropertyKey[propertyIdx] = catalog.insertRelPropertyKeyIfNeeded(
                    propertyDataType[0], dataType);
            }
            catalog.setNumPropertiesForLabel(label, dataTypes.length - 2);
            relPropertyDescriptions[label] = new Pair<>(dataTypes, indexToPropertyKey);
        }
        logger.info(String.format("Exiting [readRelFileHeaders], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    private void countRels(long[][] numLinesPerBlock) throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [countRels]");
        logMemory();
        var numBlocks = new int[catalog.getNumLabels()];
        for (var label = 0; label < catalog.getNumLabels(); label++) {
            var numBlocksOfALabel = 1 + (int) (new File(relFiles[label]).length() /
                Configuration.getBlockSize());
            numLinesPerBlock[label] = new long[numBlocksOfALabel];
            numBlocks[label] = numBlocksOfALabel;
        }
        var taskManager = new FileBlockLinesCounterThread.FileBlockLinesCounterTaskManager(
            relFiles, numBlocks);
        var executor = Executors.newFixedThreadPool(Configuration.getMaxIOThreads());
        for (var i = 0; i < Configuration.getMaxIOThreads(); i++) {
            executor.execute(new FileBlockLinesCounterThread(separator, numLinesPerBlock, taskManager));
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        logger.info(String.format("Exiting [countRels], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    private void readRels(int[][][] edgesNbrTypesAndBucketOffsets, long[][][] edgesNbrOffsets,
        long[][] numLinesPerBlock) throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [readRels]");
        logMemory();
        var taskManager = new FileBlockLinesCounterThread.FileBlockLinesCounterTaskManager(
            relFiles, Arrays.stream(numLinesPerBlock).mapToInt(blocks -> blocks.length).toArray());
        var numThread = Configuration.getMaxComputingThreads();
        var executor = Executors.newFixedThreadPool(numThread);
        for (var i = 0; i < numThread; i++) {
            executor.execute(new RelsReaderThread(separator, numLinesPerBlock,
                edgesNbrTypesAndBucketOffsets, edgesNbrOffsets, nodeIDMapping, taskManager));
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        if (null != outputDirectory) {
            nodeIDMapping = null;
        }
        logger.info(String.format("Exiting [readRels], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    private void fillGraphCatalog(int[][][] edgeNbrTypesAndBucketOffsets) throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [fillNodeTypeToRelLabelsMaps]");
        logMemory();
        var labelSrcTypeMap = new boolean[catalog.getNumLabels()][catalog.getNumTypes()];
        var labelDstTypeMap = new boolean[catalog.getNumLabels()][catalog.getNumTypes()];
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        for (var label = 0; label < catalog.getNumLabels(); label++) {
            for (var blockId = 0; blockId < edgeNbrTypesAndBucketOffsets[label].length; blockId++) {
                var block = edgeNbrTypesAndBucketOffsets[label][blockId];
                var finalLabel = label;
                executor.execute(() -> {
                    for (var i = 1; i < block.length; i += 3) {
                        labelDstTypeMap[finalLabel][block[i]] = true;
                    }
                });
                executor.execute(() -> {
                    for (var i = 0; i < block.length; i += 3) {
                        labelSrcTypeMap[finalLabel][block[i]] = true;
                    }
                });
            }
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        for (var type = 0; type < catalog.getNumTypes(); type++) {
            for (var label = 0; label < catalog.getNumLabels(); label++) {
                if (labelSrcTypeMap[label][type]) {
                    catalog.addTypeLabelForDirection(type, label, Direction.FORWARD);
                }
                if (labelDstTypeMap[label][type]) {
                    catalog.addTypeLabelForDirection(type, label, Direction.BACKWARD);
                }
            }
        }
        logger.info(String.format("Exiting [fillNodeTypeToRelLabelsMaps], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }
    private BucketOffsetManager[][] initBucketOffsetManagers(int[][][] edgesNbrTypesAndBucketOffsets,
        long[][][] edgesNbrOffsets) throws InterruptedException, IOException {
        logger.info("Initializing `bucketOffsetsManagers`.");
        var bucketOffsetManagers = new BucketOffsetManager[catalog.getNumLabels()][catalog.getNumTypes()];
        IntStream.range(0, catalog.getNumLabels()).filter(label ->
            catalog.labelHasProperties(label)).forEach(label -> {
            if (catalog.labelDirectionHasMultiplicityOne(label, Direction.FORWARD) ||
                !catalog.labelDirectionHasMultiplicityOne(label, Direction.BACKWARD)) {
                catalog.getLabelToTypeMapInDirection(Direction.FORWARD).get(label).forEach(srcType ->
                    bucketOffsetManagers[label][srcType] = new BucketOffsetManager(
                        numNodesPerType[srcType]));
            } else {
                catalog.getLabelToTypeMapInDirection(Direction.BACKWARD).get(label).forEach(dstType ->
                    bucketOffsetManagers[label][dstType] = new BucketOffsetManager(
                        numNodesPerType[dstType]));
            } });
        enumerateBucketOffsets(edgesNbrTypesAndBucketOffsets, edgesNbrOffsets, bucketOffsetManagers);
        if (null != outputDirectory) {
            serializeObject("bucketOffsetManagers", bucketOffsetManagers);
        } else {
            graph.setBucketOffsetManagers(bucketOffsetManagers);
        }
        return bucketOffsetManagers;
    }

    private void enumerateBucketOffsets(int[][][] edgeNbrTypesAndBucketOffsets,
        long[][][] edgeNbrOffsets, BucketOffsetManager[][] bucketOffsetManagers)
        throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [enumerateBucketOffsets]");
        logMemory();
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        IntStream.range(0, catalog.getNumLabels()).filter(label ->
            catalog.labelHasProperties(label)).forEach(label -> {
            if (catalog.labelDirectionHasMultiplicityOne(label, Direction.FORWARD)) {
                catalog.getLabelToTypeMapInDirection(Direction.FORWARD).get(label).forEach(srcType ->
                    executor.execute(new BucketOffsetsEnumeratorThread(srcType, label,
                        edgeNbrTypesAndBucketOffsets[label], edgeNbrOffsets[label],
                        bucketOffsetManagers[label][srcType], numNodesPerType[srcType],
                        EnumerationType.BY_SRC_OFFSETS)));
            } else if (catalog.labelDirectionHasMultiplicityOne(label, Direction.BACKWARD)) {
                catalog.getLabelToTypeMapInDirection(Direction.BACKWARD).get(label).forEach(dstType ->
                    executor.execute(new BucketOffsetsEnumeratorThread(dstType, label,
                        edgeNbrTypesAndBucketOffsets[label], edgeNbrOffsets[label],
                        bucketOffsetManagers[label][dstType], numNodesPerType[dstType],
                        EnumerationType.BY_DST_OFFSETS)));
            } else {
                catalog.getLabelToTypeMapInDirection(Direction.FORWARD).get(label).forEach(srcType ->
                    executor.execute(new BucketOffsetsEnumeratorThread(srcType, label,
                        edgeNbrTypesAndBucketOffsets[label], edgeNbrOffsets[label],
                        bucketOffsetManagers[label][srcType], numNodesPerType[srcType],
                        EnumerationType.BY_RELS)));
            }});
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        logger.info(String.format("Exiting [enumerateBucketOffsets], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    private void initRelStores(BucketOffsetManager[][] bucketOffsetManagers,
        long[][] numLinesPerBlock, int[][][] edgesNbrTypesAndBucketOffsets,
        long[][][] edgesNbrOffsets) throws InterruptedException, IOException {
        logger.info("Initializing `edgeStore`.");
        logMemory();
        var edgeStore = new RelPropertyStore();
        edgeStore.init(relPropertyDescriptions, bucketOffsetManagers);
        logMemory();
        readRelProperties(numLinesPerBlock, edgesNbrTypesAndBucketOffsets, edgesNbrOffsets, edgeStore);
        if (null != outputDirectory) {
            var aTime = System.nanoTime();
            logger.info("Initiating [edgeStoreSerialization]");
            logMemory();
            edgeStore.serialize(outputDirectory);
            logger.info(String.format("Exiting [edgeStoreSerialization], completed in %.2f ms",
                IOUtils.getTimeDiff(aTime)));
            logMemory();
        } else {
            graph.setRelPropertyStore(edgeStore);
        }
    }

    private void readRelProperties(long[][] numLinesPerBlock, int[][][] edgesNbrTypesAndBucketOffsets,
        long[][][] edgesNbrOffsets, RelPropertyStore relPropertyStore)
        throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [readRelProperties]");
        logMemory();
        var taskManager = new FileBlockLinesCounterThread.FileBlockLinesCounterTaskManager(
            relFiles, Arrays.stream(numLinesPerBlock).mapToInt(blocks -> blocks.length).toArray());
        var numThread = Configuration.getMaxComputingThreads();
        var executor = Executors.newFixedThreadPool(numThread);
        for (var i = 0; i < numThread; i++) {
            executor.execute(new RelPropertiesReaderThread(separator, numLinesPerBlock, relPropertyStore,
                relPropertyDescriptions, edgesNbrTypesAndBucketOffsets, edgesNbrOffsets,
                taskManager, catalog));
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        logger.info(String.format("Exiting [readRelProperties], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    private void createAdjListsIndex(int[][][] typesAndBucketOffsets, long[][][] offsets,
        Direction direction)
        throws InterruptedException, IOException {
        var typeToDefaultAdjListIndexLabelsMap =
            catalog.getTypeToDefaultAdjListIndexLabelsMapInDirection(direction);
        var typeToColumnAdjListIndexLabelsMap =
            catalog.getTypeToColumnAdjListIndexLabelsMapInDirection(direction);
        var columns = new ColumnAdjListIndex[catalog.getNumTypes()][];
        var ucAdjLists = new UncompressedAdjListGroup[catalog.getNumTypes()][][];
        initDefaultAdjListIndex(ucAdjLists, typeToDefaultAdjListIndexLabelsMap);
        initColumnAdjListIndexes(columns, direction, typeToColumnAdjListIndexLabelsMap);
        countRelsPerNodePerDefaultAdjListIndexLabel(ucAdjLists, typeToDefaultAdjListIndexLabelsMap,
            typesAndBucketOffsets, offsets, direction);
        initAdjLists(ucAdjLists);
        populateAdjLists(ucAdjLists, columns, typeToDefaultAdjListIndexLabelsMap,
            typeToColumnAdjListIndexLabelsMap, typesAndBucketOffsets, offsets, direction);
        var defaultAdjListIndexes = new DefaultAdjListIndex[catalog.getNumTypes()][];
        compressAdjListIndex(ucAdjLists, columns, defaultAdjListIndexes, direction);
        logMemory();
        if (null != outputDirectory) {
            var aTime = System.nanoTime();
            logger.info("Initiating [" + direction + " adjList indexes serialization]");
            logMemory();
            AdjListIndexes.serializeDefaultAdjListIndexes(outputDirectory, defaultAdjListIndexes,
                direction + "DefaultAdjListIndexes");
            AdjListIndexes.serializeColumnAdjListIndexes(outputDirectory, columns,
                direction + "ColumnAdjListIndexes");
            logger.info(String.format("Exiting [%s adjList indexes serialization], " +
                "completed in %.2f ms", direction, IOUtils.getTimeDiff(aTime)));
            logMemory();
        } else {
            if (Direction.FORWARD == direction) {
                adjListIndexes.setFwdDefaultAdjListIndexes(defaultAdjListIndexes);
                adjListIndexes.setFwdColumnAdjListIndexes(columns);
            } else {
                adjListIndexes.setBwdDefaultAdjListIndexes(defaultAdjListIndexes);
                adjListIndexes.setBwdColumnAdjListIndexes(columns);
            }
        }
    }

    public void initDefaultAdjListIndex(UncompressedAdjListGroup[][][] ucAdjLists,
        List<List<Integer>> typeToDefaultAdjListIndexLabelsMap) throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [initDefaultAdjListIndex]");
        logMemory();
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        var numAdjListGroupPerThread = Configuration.getNumAdjListGroupsPerThread();
        IntStream.range(0, catalog.getNumTypes()).forEach(type -> {
            var labels = typeToDefaultAdjListIndexLabelsMap.get(type);
            ucAdjLists[type] = new UncompressedAdjListGroup[labels.size()][];
            IntStream.range(0, labels.size()).forEach(labelIdx -> {
                var numGroups = 1 + ((int) (numNodesPerType[type] / Configuration.getDefaultAdjListGroupingSize()));
                ucAdjLists[type][labelIdx] = new UncompressedAdjListGroup[numGroups];
                var startGroupIdx = 0;
                var ucAdjListsOfTypeLabel = ucAdjLists[type][labelIdx];
                while (startGroupIdx < numGroups) {
                    var endGroupIdx = Math.min(startGroupIdx + numAdjListGroupPerThread, numGroups);
                    var finalStartGroupIdx = startGroupIdx;
                    executor.execute(() -> {
                        for (var i = finalStartGroupIdx; i < endGroupIdx; i++) {
                            ucAdjListsOfTypeLabel[i] = new UncompressedAdjListGroup();
                        }
                    });
                    startGroupIdx += numAdjListGroupPerThread;
                }

            });
        });
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        logger.info(String.format("Exiting [initDefaultAdjListIndex], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    private void initColumnAdjListIndexes(ColumnAdjListIndex[][] columns, Direction direction,
        List<List<Integer>> typeToColumnAdjListIndexLabelsMap) {
        var aTime = System.nanoTime();
        logger.info("Initiating [initColumnAdjListIndexes]");
        logMemory();
        IntStream.range(0, catalog.getNumTypes()).forEach(type -> {
            var labels = typeToColumnAdjListIndexLabelsMap.get(type);
            columns[type] = new ColumnAdjListIndex[labels.size()];
            IntStream.range(0, labels.size()).forEach(labelIdx -> {
                var label = labels.get(labelIdx);
                if (catalog.labelDirectionHasSingleNbrType(label, direction)) {
                    columns[type][labelIdx] = new ColumnAdjListIndexSingleType(type,
                        numNodesPerType[type]);
                } else {
                    columns[type][labelIdx] = new ColumnAdjListIndexMultiType(type,
                        numNodesPerType[type]);
                }
            });
        });
        logger.info(String.format("Exiting [initDefaultAdjListIndex], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    private void countRelsPerNodePerDefaultAdjListIndexLabel(
        UncompressedAdjListGroup[][][] ucAdjLists,
        List<List<Integer>>  defaultAdjListIndexLabelToTypeMap, int[][][] typesAndBucketOffsets,
        long[][][] offsets, Direction direction) throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [countRelsPerNodePerDefaultAdjListIndexLabel]");
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        IntStream.range(0, catalog.getNumTypes()).forEach(type -> {
            var labels = defaultAdjListIndexLabelToTypeMap.get(type);
            IntStream.range(0, labels.size()).forEach(labelIdx -> {
                var label = labels.get(labelIdx);
                executor.execute(new RelLabelPerNodeProfilerThread(label, type,
                    typesAndBucketOffsets[label], offsets[label], ucAdjLists[type][labelIdx],
                    direction));
            });
        });
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        logger.info(String.format("Exiting [countRelsPerNodePerDefaultAdjListIndexLabel]," +
            " completed in %.2f ms", IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    public void initAdjLists(UncompressedAdjListGroup[][][] ucAdjLists) throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [initAdjLists]");
        logMemory();
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        var numAdjListGroupPerThread = Configuration.getNumAdjListGroupsPerThread();
        IntStream.range(0, catalog.getNumTypes()).forEach(type ->
            IntStream.range(0, ucAdjLists[type].length).forEach(labelIdx -> {
                var numGroups = ucAdjLists[type][labelIdx].length;
                var startIdx = 0;
                var ucAdjListsOfType = ucAdjLists[type][labelIdx];
                while (startIdx < numGroups) {
                    var endIdx = Math.min(startIdx + numAdjListGroupPerThread, numGroups);
                    int finalStartIdx = startIdx;
                    executor.execute(() -> {
                        for (var i = finalStartIdx; i < endIdx; i++) {
                            ucAdjListsOfType[i].init();
                        }
                    });
                    startIdx += numAdjListGroupPerThread;
                }
            })
        );
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        logger.info(String.format("Exiting [initAdjLists], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    public void populateAdjLists(UncompressedAdjListGroup[][][] ucAdjLists,
        ColumnAdjListIndex[][] columns, List<List<Integer>> typeToDefaultAdjListIndexLabelsMap,
        List<List<Integer>> typeToColumnAdjListIndexLabelsMap, int[][][] typesAndBucketOffsets,
        long[][][] offsets, Direction direction) throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [populateAdjLists]");
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        IntStream.range(0, catalog.getNumTypes()).forEach(type -> {
            var labels = typeToDefaultAdjListIndexLabelsMap.get(type);
            IntStream.range(0, labels.size()).forEach(labelIdx -> {
                var label = labels.get(labelIdx);
                executor.execute(AdjacencyListPopulatorThread.make(label, type,
                    ucAdjLists[type][labelIdx], null, typesAndBucketOffsets[label], offsets[label],
                    direction));
            });
        });
        IntStream.range(0, catalog.getNumTypes()).forEach(type -> {
            var labels = typeToColumnAdjListIndexLabelsMap.get(type);
            IntStream.range(0, labels.size()).forEach(labelIdx -> {
                var label = labels.get(labelIdx);
                executor.execute(AdjacencyListPopulatorThread.make(label, type, null,
                    columns[type][labelIdx], typesAndBucketOffsets[label], offsets[label], direction));
            });
        });
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        logger.info(String.format("Exiting [populateAdjLists], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    public void compressAdjListIndex(UncompressedAdjListGroup[][][] ucAdjLists,
        ColumnAdjListIndex[][] columns, DefaultAdjListIndex[][] indexes, Direction direction)
        throws InterruptedException {
        var aTime = System.nanoTime();
        logger.info("Initiating [compressAdjListIndex]");
        logMemory();
        var taskManager = new AdjListSerializerTaskManager(catalog, direction, indexes, ucAdjLists);
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        for (var i = 0; i < Configuration.getMaxComputingThreads(); i++) {
            executor.execute(new AdjListSerializerThread(taskManager));
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        logger.info(String.format("Exiting [compressAdjListIndex], completed in %.2f ms",
            IOUtils.getTimeDiff(aTime)));
        logMemory();
    }

    private void serializeObject(String filename, Object object) throws IOException {
        logger.info(String.format("Serializing '%s'", filename));
        var outputStream = new ObjectOutputStream(new BufferedOutputStream(
            new FileOutputStream(outputDirectory + filename)));
        outputStream.writeObject(object);
        outputStream.close();
    }

    private static void logMemory() {
        logger.info(String.format(
            "----------( used memory : %.2f GB )----------",
            ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) /
                (1024.0 * 1024.0 * 1024.0))));
    }
}
