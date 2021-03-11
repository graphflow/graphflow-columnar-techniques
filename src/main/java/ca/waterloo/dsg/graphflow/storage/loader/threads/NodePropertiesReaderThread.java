package ca.waterloo.dsg.graphflow.storage.loader.threads;

import ca.waterloo.dsg.graphflow.storage.loader.NodeIDMapping;
import ca.waterloo.dsg.graphflow.storage.loader.fileiterator.FileBlockLinesIterator;
import ca.waterloo.dsg.graphflow.storage.loader.threads.FileBlockLinesCounterThread.FileBlockLinesCounterTaskManager;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.NodePropertyStore;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import ca.waterloo.dsg.graphflow.util.container.Pair;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class NodePropertiesReaderThread implements Runnable{

    private static final Logger logger = LogManager.getLogger(NodePropertiesReaderThread.class);

    long[][] numLinesInBlock;
    FileBlockLinesIterator linesIterator;
    FileBlockLinesCounterTaskManager taskManager;
    NodeIDMapping nodeIDMapping;
    NodePropertyStore nodePropertyStore;
    Pair<DataType[], int[]>[] propertyDescriptions;

    String filename;
    int type;
    int blockId;
    long nodeOffsetBegin, nodeOffsetEnd;

    public NodePropertiesReaderThread(char separator, long[][] numLinesInBlock,
        NodeIDMapping nodeIDMapping, NodePropertyStore nodePropertyStore, Pair<DataType[],
        int[]>[] propertyDescriptions, FileBlockLinesCounterTaskManager taskManager) {
        this.linesIterator = new FileBlockLinesIterator(separator);
        this.nodeIDMapping = nodeIDMapping;
        this.nodePropertyStore = nodePropertyStore;
        this.numLinesInBlock = numLinesInBlock;
        this.taskManager = taskManager;
        this.propertyDescriptions = propertyDescriptions;
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
            type = taskManager.currentFile;
            filename = taskManager.fileNames[taskManager.currentFile];
            blockId = taskManager.currentBlock;
            nodeOffsetBegin = blockId == 0 ? 0 : numLinesInBlock[type][blockId - 1];
            nodeOffsetEnd = nodeOffsetBegin + numLinesInBlock[type][blockId];
            numLinesInBlock[type][blockId] = blockId == 0 ? numLinesInBlock[type][blockId] :
                numLinesInBlock[type][blockId] + numLinesInBlock[type][blockId - 1];
            return true;
        } finally {
            taskManager.lock.unlock();
        }
    }

    private void execute() {
        var sTime = System.nanoTime();
        logger.debug(String.format("Type: %d, BlockId: %d, Lines: %d-%d", type, blockId,
            nodeOffsetBegin, nodeOffsetEnd));
        var currentVertexOffset = nodeOffsetBegin;
        try {
            linesIterator.setBlock(filename, blockId);
            if (blockId == 0 /*ignore the first line*/) {
                linesIterator.skipLine();
            }
            while (linesIterator.hasNextLine()) {
                var tokensIterator = linesIterator.nextLineTokensIterator();
                var vertexId = tokensIterator.getTokenAsLong();
                nodeIDMapping.setNode(vertexId, type, currentVertexOffset);
                nodePropertyStore.setProperties(type, currentVertexOffset, tokensIterator,
                    propertyDescriptions[type]);
                currentVertexOffset++;
            }
        } catch (IOException e) {
            logger.error("IOError occurred while reading: " + filename);
        }
        logger.debug(String.format("Type: %d, BlockId: %d, Lines: %d-%d, completed in %f.2 ms",
            type, blockId, nodeOffsetBegin, nodeOffsetEnd, IOUtils.getTimeDiff(sTime)));
    }
}
