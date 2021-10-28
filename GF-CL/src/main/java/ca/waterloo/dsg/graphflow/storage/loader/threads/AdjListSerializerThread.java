package ca.waterloo.dsg.graphflow.storage.loader.threads;

import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.DefaultAdjListIndex;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.DefaultAdjListIndexMultiType;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.DefaultAdjListIndexSingleType;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.util.AdjListSorter;
import ca.waterloo.dsg.graphflow.storage.loader.UncompressedAdjListGroup;
import ca.waterloo.dsg.graphflow.util.Configuration;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import ca.waterloo.dsg.graphflow.util.container.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.ReentrantLock;

public class AdjListSerializerThread implements Runnable {

    public static class AdjListSerializerTaskManager {

        GraphCatalog catalog;
        UncompressedAdjListGroup[][][] ucAdjLists;
        DefaultAdjListIndex[][] indexes;
        byte[][] currentBytesArray;
        int[][] currentIntsArray;
        Direction direction;
        boolean storeNbrTypes, storeBucketOffsets;
        int numGroupsForCurrentType;
        int currentType = -1;
        int currentLabelIdx = -1;
        int currentGroup;
        ReentrantLock lock = new ReentrantLock();

        public AdjListSerializerTaskManager(GraphCatalog catalog, Direction direction,
            DefaultAdjListIndex[][] indexes, UncompressedAdjListGroup[][][] ucAdjLists) {
            this.catalog = catalog;
            this.direction = direction;
            this.indexes = indexes;
            this.ucAdjLists = ucAdjLists;
        }

        private void setFlagsForLabelIdx() {
            var typeToLabelsMap = catalog.getTypeToDefaultAdjListIndexLabelsMapInDirection(direction);
            var label = typeToLabelsMap.get(currentType).get(currentLabelIdx);
            storeBucketOffsets = !catalog.labelDirectionHasMultiplicityOne(label, Direction.FORWARD) &&
                !catalog.labelDirectionHasMultiplicityOne(label, Direction.BACKWARD) &&
                catalog.labelHasProperties(label);
            storeNbrTypes = !catalog.labelDirectionHasSingleNbrType(label, direction);
        }

        private boolean done() {
            return currentType >= ucAdjLists.length;
        }

        private boolean incrementGroup() {
            if (!done()) {
                currentGroup = Math.min(numGroupsForCurrentType, currentGroup +
                    Configuration.getNumAdjListGroupsPerThread());
                if (currentGroup >= numGroupsForCurrentType) {
                    return incrementLabelIdx();
                }
                return true;
            }
            return false;
        }

        private boolean incrementLabelIdx() {
            currentLabelIdx++;
            if (currentType == -1 || currentLabelIdx >= ucAdjLists[currentType].length) {
                if (!incrementType()) {
                    return false;
                }
            }
            currentGroup = 0;
            numGroupsForCurrentType = ucAdjLists[currentType][currentLabelIdx].length;
            setFlagsForLabelIdx();
            currentIntsArray = new int[numGroupsForCurrentType][];
            if (storeNbrTypes) {
                currentBytesArray = new byte[numGroupsForCurrentType][];
                indexes[currentType][currentLabelIdx] = new DefaultAdjListIndexMultiType(
                    currentIntsArray, currentBytesArray);
            } else {
                currentBytesArray = null;
                indexes[currentType][currentLabelIdx] = new DefaultAdjListIndexSingleType(
                    currentIntsArray);
            }
            return true;
        }

        private boolean incrementType() {
            do {
                currentType++;
            } while (!done() && 0 == ucAdjLists[currentType].length);
            if (done()) {
                return false;
            }
            currentLabelIdx = 0;
            currentGroup = 0;
            indexes[currentType] = new DefaultAdjListIndex[ucAdjLists[currentType].length];
            return true;
        }

        private int getEndGroup() {
            return Math.min(numGroupsForCurrentType, currentGroup +
                Configuration.getNumAdjListGroupsPerThread());
        }
    }

    private static final Logger logger = LogManager.getLogger(AdjListSerializerThread.class);

    int startGroupId;
    int endGroupId;
    UncompressedAdjListGroup[] adjListGroups;
    byte[][] bytesArrays;
    int[][] intsArrays;
    boolean storeNbrTypes, storeBucketOffsets;

    AdjListSorter sorter;
    Pair<Integer, Integer> currentArrayCursors;
    int[] currentIntsArray;
    byte[] currentBytesArray;
    AdjListSerializerTaskManager taskManager;

    public AdjListSerializerThread(AdjListSerializerTaskManager taskManager) {
        this.taskManager = taskManager;
        this.currentArrayCursors = new Pair<>(0, 0);
        this.sorter = new AdjListSorter();
    }

    @Override
    public void run() {
        while (getNextGroupPartition()) {
            execute();
        }
    }

    private boolean getNextGroupPartition() {
        taskManager.lock.lock();
        try {
            if (!taskManager.incrementGroup()) {
                return false;
            }
            setValuesFromTaskManager();
            return true;
        } finally {
            taskManager.lock.unlock();
        }
    }

    private void setValuesFromTaskManager() {
        startGroupId = taskManager.currentGroup;
        endGroupId = taskManager.getEndGroup();
        adjListGroups = taskManager.ucAdjLists[taskManager.currentType][taskManager.currentLabelIdx];
        bytesArrays = taskManager.currentBytesArray;
        intsArrays = taskManager.currentIntsArray;
        storeBucketOffsets = taskManager.storeBucketOffsets;
        storeNbrTypes = taskManager.storeNbrTypes;
    }

    private void execute() {
        var sTime = System.nanoTime();
        logger.debug(String.format("GroupId Range: %d-%d", startGroupId, endGroupId));
        for (var groupId = startGroupId; groupId < endGroupId; groupId++) {
            encodeAdjListGroup(adjListGroups[groupId]);
            intsArrays[groupId] = currentIntsArray;
            if (storeNbrTypes) {
                bytesArrays[groupId] = currentBytesArray;
            }
        }
        logger.debug(String.format("GroupId Range: %d-%d, completed in %f.2 ms", startGroupId,
            endGroupId, IOUtils.getTimeDiff(sTime)));
    }

    public void encodeAdjListGroup(UncompressedAdjListGroup adjListGroup) {
        currentArrayCursors.a = 0;
        currentArrayCursors.b = 0;
        getArraySizesForAdjListGroup(adjListGroup);
        if (currentArrayCursors.b > 0) {
            currentIntsArray = new int[currentArrayCursors.b +
                Configuration.getDefaultAdjListGroupingSize() + 1];
            if (currentArrayCursors.a > 0) {
                currentBytesArray = new byte[currentArrayCursors.a];
            } else {
                currentBytesArray = null;
            }
            fillArraysInAdjListGroup(adjListGroup);
        } else {
            currentIntsArray = null;
            currentBytesArray = null;
        }
    }

    private void getArraySizesForAdjListGroup(UncompressedAdjListGroup adjListGroup) {
        var numRels = adjListGroup.getNumRels();
        if (0 != numRels) {
            currentArrayCursors.a = storeNbrTypes ? numRels : 0;
            currentArrayCursors.b += storeBucketOffsets ? numRels * 2 : numRels;
        }
    }

    private void fillArraysInAdjListGroup(UncompressedAdjListGroup adjListGroup) {
        sorter.sortAdjList(adjListGroup);
        currentArrayCursors.a = 0;
        currentArrayCursors.b = 0;
        var edgesCursor = Configuration.getDefaultAdjListGroupingSize() + 1;
        var vertexStartIdx = 0;
        for (var i = 0; i < Configuration.getDefaultAdjListGroupingSize(); i++) {
            var vertexEndIdx = adjListGroup.getMetadata()[i];
            currentIntsArray[currentArrayCursors.b++] = currentArrayCursors.a;
            for (var k = vertexStartIdx; k < vertexEndIdx; k++) {
                currentIntsArray[edgesCursor++] = (int) adjListGroup.getOffsets()[k];
                if (storeBucketOffsets) {
                    currentIntsArray[edgesCursor++] = adjListGroup.getTypesAndBucketOffsets()[1 + (k << 1)];
                }
                if (storeNbrTypes) {
                    currentBytesArray[currentArrayCursors.a] = (byte) adjListGroup.getTypesAndBucketOffsets()[k << 1];
                }
                currentArrayCursors.a++;
            }
            vertexStartIdx = vertexEndIdx;
        }
        currentIntsArray[currentArrayCursors.b++] = currentArrayCursors.a;
    }
}
