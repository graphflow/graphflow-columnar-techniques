package ca.waterloo.dsg.graphflow.storage.loader;

import ca.waterloo.dsg.graphflow.util.collection.ArrayUtils;
import lombok.Getter;

public class NodeIDMapping {

    @Getter
    private int[][] types;
    @Getter private long[][] offsets;

    NodeIDMapping(long numNodes) {
        offsets = ArrayUtils.create2DLongArray(numNodes + 1);
        types = ArrayUtils.create2DIntegerArray(numNodes + 1);
    }

    public void setNode(long nodeID, int type, long offset) {
        types[(int) nodeID / Integer.MAX_VALUE][(int) nodeID % Integer.MAX_VALUE]
            = type;
        offsets[(int) nodeID / Integer.MAX_VALUE][(int) nodeID % Integer.MAX_VALUE]
            = offset;
    }

    public int getNodeType(long nodeID) {
        return types[(int) nodeID / Integer.MAX_VALUE][(int) nodeID % Integer.MAX_VALUE];
    }

    public long getNodeOffset(long nodeID) {
        return offsets[(int) nodeID / Integer.MAX_VALUE][(int) nodeID % Integer.MAX_VALUE];
    }
}
