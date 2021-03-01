package ca.waterloo.dsg.graphflow.storage.loader.threads;

import ca.waterloo.dsg.graphflow.storage.loader.NodeIDMapping;
import ca.waterloo.dsg.graphflow.storage.loader.fileiterator.FileBlockLinesIterator;
import ca.waterloo.dsg.graphflow.storage.loader.threads.FileBlockLinesCounterThread.FileBlockLinesCounterTaskManager;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

public class RelsReaderThread implements Runnable {

    private static final Logger logger = LogManager.getLogger(RelsReaderThread.class);

    public NodeIDMapping nodeIDMapping;
    FileBlockLinesIterator linesIterator;
    FileBlockLinesCounterTaskManager taskManager;
    long[][] numLinesInBlock;
    int[][][] typesAndBucketOffsets;
    long[][][] offsets;

    String filename;
    int label;
    int blockId;

    public RelsReaderThread(char separator, long[][] numLinesPerBlock,
        int[][][] edgesNbrTypesAndBucketOffsets, long[][][] edgesNbrOffsets,
        NodeIDMapping nodeIDMapping, FileBlockLinesCounterTaskManager taskManager) {
        this.typesAndBucketOffsets = edgesNbrTypesAndBucketOffsets;
        this.offsets = edgesNbrOffsets;
        this.linesIterator = new FileBlockLinesIterator(separator);
        this.taskManager = taskManager;
        this.numLinesInBlock = numLinesPerBlock;
        this.nodeIDMapping = nodeIDMapping;
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
            label = taskManager.currentFile;
            filename = taskManager.fileNames[taskManager.currentFile];
            blockId = taskManager.currentBlock;
            if (blockId == 0) {
                typesAndBucketOffsets[label] = new int[numLinesInBlock[label].length][];
                offsets[label] = new long[numLinesInBlock[label].length][];
            }
            typesAndBucketOffsets[label][blockId] = new int[3 * (int) numLinesInBlock[label][blockId]];
            Arrays.fill(typesAndBucketOffsets[label][blockId], -1);
            offsets[label][blockId] = new long[2 * (int) numLinesInBlock[label][blockId]];
            return true;
        } finally {
            taskManager.lock.unlock();
        }
    }

    private void execute() {
        var sTime = System.nanoTime();
        logger.debug(String.format("Filename: %s, BlockId: %d", filename, blockId));
        var i = 0;
        var j = 0;
        try {
            linesIterator.setBlock(filename, blockId);
            if (blockId == 0) {
                linesIterator.skipLine();
            }
            var typesAndBucketOffsetsBlock = typesAndBucketOffsets[label][blockId];
            var offsetsBlock = offsets[label][blockId];
            while (linesIterator.hasNextLine()) {
                var tokensIterator = linesIterator.nextLineTokensIterator();
                var srcVertexId = tokensIterator.getTokenAsLong();
                var dstVertexId = tokensIterator.getTokenAsLong();
                typesAndBucketOffsetsBlock[i] = nodeIDMapping.getNodeType(srcVertexId);
                typesAndBucketOffsetsBlock[1 + i] = nodeIDMapping.getNodeType(dstVertexId);
                offsetsBlock[j] = nodeIDMapping.getNodeOffset(srcVertexId);
                offsetsBlock[1 + j] = nodeIDMapping.getNodeOffset(dstVertexId);
                i += 3;
                j += 2;
            }
        } catch (IOException e) {
            logger.error("IOError occurred while reading: " + filename);
        }
        logger.debug(String.format("Filename %s, BlockId: %d, Completed in %f ms", filename,
            blockId, IOUtils.getTimeDiff(sTime)));
    }
}
