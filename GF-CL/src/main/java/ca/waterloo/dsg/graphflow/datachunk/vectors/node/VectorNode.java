package ca.waterloo.dsg.graphflow.datachunk.vectors.node;

import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorIterator;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;

public class VectorNode extends Vector {

    private int nodeType;
    private final int[] nodeOffsets;

    public VectorNode(int capacity) {
        nodeOffsets = new int[capacity];
    }

    @Override
    public int getNodeType(int pos) {
        return nodeType;
    }

    @Override
    public int getNodeOffset(int pos) {
        return nodeOffsets[pos];
    }

    @Override
    public void setNodeType(int nodeType) {
        this.nodeType = nodeType;
    }

    @Override
    public void setNodeOffset(int pos, int nodeOffsets) {
        this.nodeOffsets[pos] = nodeOffsets;
    }

    @Override
    public int[] getNodeOffsets() {
        return nodeOffsets;
    }

    @Override
    public VectorIterator getIterator() {
        return new VectorNode.VectorNodesIterator(this);
    }

    public static class VectorNodesIterator implements VectorIterator {

        private final VectorNode vector;
        private int pos;

        VectorNodesIterator(VectorNode vector) {
            this.vector = vector;
        }

        @Override
        public void init() {
            this.pos = 0;
        }

        @Override
        public int getNextNodeOffset() {
            return vector.nodeOffsets[pos];
        }

        @Override
        public void moveCursor() {
            pos++;
        }
    }
}
