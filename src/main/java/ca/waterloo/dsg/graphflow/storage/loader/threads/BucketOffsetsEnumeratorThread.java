package ca.waterloo.dsg.graphflow.storage.loader.threads;

import ca.waterloo.dsg.graphflow.storage.BucketOffsetManager;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BucketOffsetsEnumeratorThread implements Runnable {

    public enum EnumerationType {
        BY_SRC_OFFSETS,
        BY_DST_OFFSETS,
        BY_RELS
    }

    private static final Logger logger = LogManager.getLogger(BucketOffsetsEnumeratorThread.class);

    int[][] typesAndBucketOffsets;
    long[][] offsets;
    BucketOffsetManager bucketOffsetManager;
    int type;
    long numVertices;
    int label;
    EnumerationType enumerationType;

    public BucketOffsetsEnumeratorThread(int type, int label, int[][] typesAndBucketOffsets,
        long[][] offsets, BucketOffsetManager bucketOffsetManager, long numVertices,
        EnumerationType enumerationType) {
        this.type = type;
        this.numVertices = numVertices;
        this.label = label;
        this.typesAndBucketOffsets = typesAndBucketOffsets;
        this.offsets = offsets;
        this.bucketOffsetManager = bucketOffsetManager;
        this.enumerationType = enumerationType;
    }

    @Override
    public void run() {
        var sTime = System.nanoTime();
        logger.debug(String.format("Label: %d, Type %d", label, type));
        if (EnumerationType.BY_RELS == enumerationType) {
            enumerateByRels();
        } else {
            enumerateBySrcOrDstOffsets();
        }
        logger.debug(String.format("Label: %d, SrcType %d, completed in %.2f", label, type,
            IOUtils.getTimeDiff(sTime)));
    }

    private void enumerateByRels() {
        for (var blockId = 0; blockId < typesAndBucketOffsets.length; blockId++) {
            var typesAndBucketOffsetsBlock = typesAndBucketOffsets[blockId];
            var k = 0;
            for (var j = 0; j < typesAndBucketOffsetsBlock.length; j += 3, k += 2) {
                if (typesAndBucketOffsetsBlock[j] == type) {
                    typesAndBucketOffsetsBlock[j + 2] = bucketOffsetManager.addElementToBucket(
                        BucketOffsetManager.getBucketID(offsets[blockId][k]));
                }
            }
        }
    }

    private void enumerateBySrcOrDstOffsets() {
        for (var i = 0; i < numVertices; i++) {
            bucketOffsetManager.addElementToBucket(BucketOffsetManager.getBucketID(i));
        }
        if (EnumerationType.BY_SRC_OFFSETS == enumerationType) {
            enumerateBySrcOffsets();
        } else {
            enumerateByDstOffsets();
        }
    }

    private void enumerateBySrcOffsets() {
        for (var blockId = 0; blockId < typesAndBucketOffsets.length; blockId++) {
            var typesAndBucketOffsetsBlock = typesAndBucketOffsets[blockId];
            var k = 0;
            for (var j = 0; j < typesAndBucketOffsetsBlock.length; j += 3, k += 2) {
                if (typesAndBucketOffsetsBlock[j] == type) {
                    typesAndBucketOffsetsBlock[j + 2] = BucketOffsetManager.getBucketOffset(
                        offsets[blockId][k]);
                }
            }
        }
    }

    private void enumerateByDstOffsets() {
        for (var blockId = 0; blockId < typesAndBucketOffsets.length; blockId++) {
            var typesAndBucketOffsetsBlock = typesAndBucketOffsets[blockId];
            var k = 0;
            for (var j = 0; j < typesAndBucketOffsetsBlock.length; j += 3, k += 2) {
                if (typesAndBucketOffsetsBlock[j + 1] == type) {
                    typesAndBucketOffsetsBlock[j + 2] = BucketOffsetManager.getBucketOffset(
                        offsets[blockId][k + 1]);
                }
            }
        }
    }
}
