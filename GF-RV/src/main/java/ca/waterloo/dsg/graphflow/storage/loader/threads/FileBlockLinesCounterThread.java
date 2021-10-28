package ca.waterloo.dsg.graphflow.storage.loader.threads;

import ca.waterloo.dsg.graphflow.storage.loader.fileiterator.FileBlockLinesIterator;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

public class FileBlockLinesCounterThread implements Runnable {

    public static class FileBlockLinesCounterTaskManager {

        String[] fileNames;
        int[] numBlocks;
        int currentFile = -1;
        int currentBlock;
        ReentrantLock lock = new ReentrantLock();

        public FileBlockLinesCounterTaskManager(String[] fileNames, int[] numBlocks) {
            this.fileNames = fileNames;
            this.numBlocks = numBlocks;
        }

        private boolean done() {
            return currentFile >= fileNames.length;
        }

        public boolean incrementBlock() {
            if (!done()) {
                currentBlock++;
                if (currentFile == -1 || currentBlock >= numBlocks[currentFile]) {
                    return incrementFile();
                }
                return true;
            }
            return false;
        }

        private boolean incrementFile() {
            currentFile++;
            if (done()) {
                return false;
            }
            currentBlock = 0;
            return true;
        }
    }

    private static final Logger logger = LogManager.getLogger(FileBlockLinesCounterThread.class);

    long[][] numLinesInBlock;
    FileBlockLinesIterator linesIterator;
    FileBlockLinesCounterTaskManager taskManager;

    String filename;
    int typeOrLabel;
    int blockId;

    public FileBlockLinesCounterThread(char separator, long[][] numLinesInBlock,
        FileBlockLinesCounterTaskManager taskManager) {
        this.linesIterator = new FileBlockLinesIterator(separator);
        this.numLinesInBlock = numLinesInBlock;
        this.taskManager = taskManager;
    }

    @Override
    public void run() {
        while (getNewBlock()) {
            execute();
        }
    }

    private boolean getNewBlock() {
        taskManager.lock.lock();
        try {
            if (!taskManager.incrementBlock()) {
                return false;
            }
            typeOrLabel = taskManager.currentFile;
            filename = taskManager.fileNames[taskManager.currentFile];
            blockId = taskManager.currentBlock;
            return true;
        } finally {
            taskManager.lock.unlock();
        }
    }

    private void execute() {
        var sTime = System.nanoTime();
        logger.debug(String.format("Filename: %s, BlockId: %d", filename, blockId));
        try {
            linesIterator.setBlock(filename, blockId);
            var num = 0L;
            while (linesIterator.hasNextLine()) {
                linesIterator.skipLine();
                num++;
            }
            numLinesInBlock[typeOrLabel][blockId] = num;
        } catch (IOException e) {
            logger.error("IOError occurred while reading: " + filename);
        }
        logger.debug(String.format("Filename: %s, BlockId: %d, completed in %.2f ms", filename,
            blockId, IOUtils.getTimeDiff(sTime)));
    }
}
