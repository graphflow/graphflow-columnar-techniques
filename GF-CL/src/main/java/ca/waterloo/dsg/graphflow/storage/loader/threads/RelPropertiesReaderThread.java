package ca.waterloo.dsg.graphflow.storage.loader.threads;

import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.loader.fileiterator.FileBlockLinesIterator;
import ca.waterloo.dsg.graphflow.storage.loader.threads.BucketOffsetsEnumeratorThread.EnumerationType;
import ca.waterloo.dsg.graphflow.storage.loader.threads.FileBlockLinesCounterThread.FileBlockLinesCounterTaskManager;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.RelPropertyStore;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import ca.waterloo.dsg.graphflow.util.container.Pair;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class RelPropertiesReaderThread implements Runnable {

    private static final Logger logger = LogManager.getLogger(RelPropertiesReaderThread.class);

    long[][] numLinesInBlock;
    FileBlockLinesIterator linesIterator;
    FileBlockLinesCounterTaskManager taskManager;
    RelPropertyStore relPropertyStore;
    Pair<DataType[], int[]>[] propertyDescriptions;
    int[][][] typesAndBucketOffsets;
    long[][][] offsets;
    GraphCatalog catalog;
    EnumerationType enumerationType;

    String filename;
    int label;
    int blockId;

    public RelPropertiesReaderThread(char separator, long[][] numLinesInBlock, RelPropertyStore relPropertyStore,
        Pair<DataType[], int[]>[] propertyDescriptions, int[][][] typesAndBucketOffsets,
        long[][][] offsets, FileBlockLinesCounterTaskManager taskManager, GraphCatalog catalog) {
        this.linesIterator = new FileBlockLinesIterator(separator);
        this.taskManager = taskManager;
        this.relPropertyStore = relPropertyStore;
        this.typesAndBucketOffsets = typesAndBucketOffsets;
        this.offsets = offsets;
        this.numLinesInBlock = numLinesInBlock;
        this.propertyDescriptions = propertyDescriptions;
        this.catalog = catalog;
    }

    @Override
    public void run() {
        while (getNextBlock()) {
            execute();
        }
    }

    private boolean getNextBlock() {
        taskManager.lock.lock();
        try {
            if (!taskManager.incrementBlock()) {
                return false;
            }
            while (!catalog.labelHasProperties(taskManager.currentFile)) {
                if (!taskManager.incrementBlock()) {
                    return false;
                }
            }
            filename = taskManager.fileNames[taskManager.currentFile];
            if (!catalog.labelDirectionHasMultiplicityOne(taskManager.currentFile, Direction.FORWARD) &&
                catalog.labelDirectionHasMultiplicityOne(taskManager.currentFile, Direction.BACKWARD)) {
                enumerationType = EnumerationType.BY_DST_OFFSETS;
            } else {
                enumerationType = EnumerationType.BY_SRC_OFFSETS;
            }
            label = taskManager.currentFile;
            blockId = taskManager.currentBlock;
            return true;
        } finally {
            taskManager.lock.unlock();
        }
    }

    private void execute() {
        var sTime = System.nanoTime();
        logger.debug(String.format("Label: %d, BlockId: %d", label, blockId));
        try {
            var typesAndBucketOffsetsInALabelBlock = typesAndBucketOffsets[label][blockId];
            var offsetsInALabelBlock = offsets[label][blockId];
            linesIterator.setBlock(filename, blockId);
            if (blockId == 0) {
                linesIterator.skipLine();
            }
            var i = 0;
            var j = 0;
            if (EnumerationType.BY_SRC_OFFSETS == enumerationType) {
                while (linesIterator.hasNextLine()) {
                    var tokenIterator = linesIterator.nextLineTokensIterator();
                    relPropertyStore.setProperties(label, typesAndBucketOffsetsInALabelBlock[i],
                        offsetsInALabelBlock[j], typesAndBucketOffsetsInALabelBlock[i + 2],
                        tokenIterator, propertyDescriptions[label]);
                    i += 3;
                    j += 2;
                }
            } else {
                while (linesIterator.hasNextLine()) {
                    var tokenIterator = linesIterator.nextLineTokensIterator();
                    relPropertyStore.setProperties(label, typesAndBucketOffsetsInALabelBlock[i + 1],
                        offsetsInALabelBlock[j + 1], typesAndBucketOffsetsInALabelBlock[i + 2],
                        tokenIterator, propertyDescriptions[label]);
                    i += 3;
                    j += 2;
                }
            }
        } catch (IOException e) {
            logger.error("IOError occurred while reading: " + filename);
        }
        logger.debug(String.format("Label: %d, BlockId: %d, completed in %.2f ms", label, blockId,
            IOUtils.getTimeDiff(sTime)));
    }
}
