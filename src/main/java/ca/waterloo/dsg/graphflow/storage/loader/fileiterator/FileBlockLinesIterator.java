package ca.waterloo.dsg.graphflow.storage.loader.fileiterator;

import ca.waterloo.dsg.graphflow.util.Configuration;
import lombok.Getter;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class FileBlockLinesIterator {

    long blockSize;
    int lineSize;
    @Getter private byte[][] bufferPairs;
    private LineTokensIterator tokensIterator;
    long startByteIdx, endByteIdx;
    BufferedInputStream reader;
    private boolean isNextLineAvailable;
    private boolean isEndOfBlock;
    private int startBufferPointer, endBufferPointer;

    public FileBlockLinesIterator(char separator) {
        this.blockSize = Configuration.getBlockSize();
        this.lineSize = Configuration.getLineSize();
        bufferPairs = new byte[2][lineSize];
        tokensIterator = new LineTokensIterator(separator, bufferPairs);
    }

    public void setBlock(String filename, int blockId) throws IOException {
        startByteIdx = blockId * blockSize;
        endByteIdx = (blockId + 1) * blockSize;
        var randomAccessFile = new RandomAccessFile(filename, "r");
        reader = new BufferedInputStream(new FileInputStream(randomAccessFile.getFD()), 1 << 13);
        boolean isBeginningOfLine = false;
        if (startByteIdx == 0) {
            isBeginningOfLine = true;
        } else {
            randomAccessFile.seek(startByteIdx - 1);
            var lastElement = reader.read();
            if (lastElement == '\n') {
                isBeginningOfLine = true;
            }
        }
        if (!isBeginningOfLine) {
            while (reader.read() != '\n') {
                startByteIdx++;
            }
            startByteIdx++;
        }
        isNextLineAvailable = false;
        isEndOfBlock = false;
        startBufferPointer = 0;
        endBufferPointer = 0;
        ensureBufferIsFilled();
    }

    public boolean hasNextLine() throws IOException {
        if (isEndOfBlock) {
            return false;
        } else if (isNextLineAvailable) {
            return true;
        } else if (startByteIdx < endByteIdx && parseNextLine()) {
            isNextLineAvailable = true;
            return true;
        }
        reader.close();
        isEndOfBlock = true;
        return false;
    }

    public void skipLine() throws IOException {
        if (isNextLineAvailable) {
            startByteIdx += (endBufferPointer - startBufferPointer);
            isNextLineAvailable = false;
            return;
        }
        // else
        hasNextLine();
        if (isNextLineAvailable) {
            skipLine();
            return;
        }
        throw new IllegalStateException("no more lines left in the block");
    }

    public LineTokensIterator nextLineTokensIterator() throws IOException {
        if (isNextLineAvailable) {
            startByteIdx += (endBufferPointer - startBufferPointer);
            isNextLineAvailable = false;
            tokensIterator.init(startBufferPointer, endBufferPointer - 1);
            return tokensIterator;
        }
        // else
        hasNextLine();
        if (isNextLineAvailable) {
            return nextLineTokensIterator();
        }
        throw new IllegalStateException("no more lines left in the block");
    }

    private boolean parseNextLine() throws IOException {
        if (endBufferPointer >= lineSize) {
            endBufferPointer -= lineSize;
            var tmp = bufferPairs[0];
            bufferPairs[0] = bufferPairs[1];
            bufferPairs[1] = tmp;
        }
        startBufferPointer = endBufferPointer;
        while (true) {
            byte nextChar = 0;
            nextChar = bufferPairs[endBufferPointer / lineSize][endBufferPointer % lineSize];
            endBufferPointer++;
            ensureBufferIsFilled();
            if ('\n' == nextChar) {
                return true;
            }
            if (0 /* null char */ == nextChar) {
                isEndOfBlock = true;
                // return true, if there is some data on the last line.
                return endBufferPointer - startBufferPointer > 1;
            }
        }
    }

    private void ensureBufferIsFilled() throws IOException {
        if (endBufferPointer == lineSize) {
            var numCharsRead = reader.read(bufferPairs[1], 0, lineSize);
            if (numCharsRead != lineSize) {
                Arrays.fill(bufferPairs[1], numCharsRead, lineSize, (byte) 0);
            }
        } else if (endBufferPointer == 0) {
            var numCharsRead = reader.read(bufferPairs[0], 0, lineSize);
            if (numCharsRead != lineSize) {
                Arrays.fill(bufferPairs[0], numCharsRead, lineSize, (byte) 0);
            }
        }
    }
}
