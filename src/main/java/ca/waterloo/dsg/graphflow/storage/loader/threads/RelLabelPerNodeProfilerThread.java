package ca.waterloo.dsg.graphflow.storage.loader.threads;

import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.loader.UncompressedAdjListGroup;
import ca.waterloo.dsg.graphflow.util.Configuration;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RelLabelPerNodeProfilerThread implements Runnable {

    private static final Logger logger = LogManager.getLogger(RelLabelPerNodeProfilerThread.class);

    int label;
    int type;
    int[][] typesAndBucketOffsets;
    long[][] offsets;
    UncompressedAdjListGroup[] adjLists;
    Direction direction;

    public RelLabelPerNodeProfilerThread(int label, int type, int[][] typesAndBucketOffsets,
        long[][] offsets, UncompressedAdjListGroup[] adjLists, Direction direction) {
        this.label = label;
        this.type = type;
        this.typesAndBucketOffsets = typesAndBucketOffsets;
        this.offsets = offsets;
        this.adjLists = adjLists;
        this.direction = direction;
    }

    @Override
    public void run() {
        var sTime = System.nanoTime();
        logger.debug(String.format("Label: %d, Type: %d Direction: %s", label, type, direction));
        if (Direction.FORWARD == direction) {
            for (var blockId = 0; blockId < typesAndBucketOffsets.length; blockId++) {
                var typesAndBucketOffsetsBlock = typesAndBucketOffsets[blockId];
                var offsetsBlock = offsets[blockId];
                var j = 0;
                for (var i = 0; i < typesAndBucketOffsetsBlock.length; i += 3, j += 2) {
                    if (typesAndBucketOffsetsBlock[i] == type) {
                        adjLists[(int) offsetsBlock[j] / Configuration.getDefaultAdjListGroupingSize()]
                            .incrementCount((int) offsetsBlock[j] %
                                Configuration.getDefaultAdjListGroupingSize());
                    }
                }
            }
        } else {
            for (var blockId = 0; blockId < typesAndBucketOffsets.length; blockId++) {
                var typesAndBucketOffsetsBlock = typesAndBucketOffsets[blockId];
                var offsetsBlock = offsets[blockId];
                var j = 1;
                for (var i = 1; i < typesAndBucketOffsetsBlock.length; i += 3, j += 2) {
                    if (typesAndBucketOffsetsBlock[i] == type) {
                        adjLists[(int) offsetsBlock[j] / Configuration.getDefaultAdjListGroupingSize()]
                            .incrementCount((int) offsetsBlock[j] %
                                Configuration.getDefaultAdjListGroupingSize());
                    }
                }
            }
        }
        logger.debug(String.format("Label: %d, Type: %d Direction: %s, completed in %.2f ms", label,
            type, direction, IOUtils.getTimeDiff(sTime)));
    }
}
