package ca.waterloo.dsg.graphflow.storage.loader.threads;

import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex.ColumnAdjListIndex;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex.ColumnAdjListIndexMultiType;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex.ColumnAdjListIndexSingleType;
import ca.waterloo.dsg.graphflow.storage.loader.UncompressedAdjListGroup;
import ca.waterloo.dsg.graphflow.util.Configuration;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AdjacencyListPopulatorThread implements Runnable {

    private static final Logger logger = LogManager.getLogger(AdjacencyListPopulatorThread.class);

    int label;
    int type;
    UncompressedAdjListGroup[] adjLists;
    ColumnAdjListIndex column;
    int[][] typeAndBucketOffsets;
    long[][] offsets;
    Direction direction;

    public AdjacencyListPopulatorThread(int label, int type, UncompressedAdjListGroup[] adjLists,
        ColumnAdjListIndex column, int[][] typeAndBucketOffsets, long[][] offsets, Direction direction) {
        this.label = label;
        this.type = type;
        this.adjLists = adjLists;
        this.column = column;
        this.typeAndBucketOffsets = typeAndBucketOffsets;
        this.offsets = offsets;
        this.direction = direction;
    }

    @Override
    public void run() {
        var sTime = System.nanoTime();
        logger.debug(String.format("Label: %d, Type: %d, Direction: %s", label, type, direction));
        if (Direction.FORWARD == direction) {
            for (var blockId = 0; blockId < offsets.length; blockId++) {
                var typesAndBucketOffsetsBlock = typeAndBucketOffsets[blockId];
                var offsetsBlock = offsets[blockId];
                var j = 0;
                for (var i = 0; i < typesAndBucketOffsetsBlock.length; i += 3, j += 2) {
                    if (typesAndBucketOffsetsBlock[i] == type) {
                        addRel(offsetsBlock[j], typesAndBucketOffsetsBlock[i + 1],
                            offsetsBlock[j + 1], typesAndBucketOffsetsBlock[i + 2], label);
                    }
                }
            }
        } else {
            for (var blockId = 0; blockId < offsets.length; blockId++) {
                var typesAndBucketOffsetsBlock = typeAndBucketOffsets[blockId];
                var offsetsBlock = offsets[blockId];
                var j = 0;
                for (var i = 0; i < typesAndBucketOffsetsBlock.length; i += 3, j += 2) {
                    if (typesAndBucketOffsetsBlock[i + 1] == type) {
                        addRel(offsetsBlock[j + 1], typesAndBucketOffsetsBlock[i],
                            offsetsBlock[j], typesAndBucketOffsetsBlock[i + 2], label);
                    }
                }
            }
        }
        logger.debug(String.format("Label: %d, Type: %d, Direction: %s, completed in %f.2 ms",
            label, type, direction, IOUtils.getTimeDiff(sTime)));
    }

    abstract void addRel(long boundVertexOffset, int type, long offset, int bucketOffset,
        int label);

    public static AdjacencyListPopulatorThread make(int label, int type,
        UncompressedAdjListGroup[] adjLists, ColumnAdjListIndex column, int[][] typeAndBucketOffsets,
        long[][] offsets, Direction direction) {
        if (column == null) {
            return new AdjacencyListPopulatorThread(label, type, adjLists, column,
                typeAndBucketOffsets, offsets, direction) {
                @Override
                void addRel(long boundVertexOffset, int type, long offset, int bucketOffset,
                    int label) {
                    adjLists[(int) boundVertexOffset / Configuration.getDefaultAdjListGroupingSize()]
                        .addRel((int) boundVertexOffset % Configuration.getDefaultAdjListGroupingSize(),
                            type, offset, bucketOffset);
                }
            };
        } else if (column.getClass().equals(ColumnAdjListIndexSingleType.class)) {
            return new AdjacencyListPopulatorThread(label, type, adjLists, column,
                typeAndBucketOffsets, offsets, direction) {
                @Override
                void addRel(long boundVertexOffset, int type, long offset, int bucketOffset,
                    int label) {
                    column.setRel(boundVertexOffset, (int) offset);
                }
            };
        } else if (column.getClass().equals(ColumnAdjListIndexMultiType.class)) {
            return new AdjacencyListPopulatorThread(label, type, adjLists, column,
                typeAndBucketOffsets, offsets, direction) {
                @Override
                void addRel(long boundVertexOffset, int type, long offset, int bucketOffset,
                    int label) {
                    column.setRel(boundVertexOffset, (int) offset, (byte) type);
                }
            };
        }
        throw new IllegalStateException(String.format("No suitable AdjListPopulator instance for " +
            " label:%d, type:%d", label, type));
    }
}
