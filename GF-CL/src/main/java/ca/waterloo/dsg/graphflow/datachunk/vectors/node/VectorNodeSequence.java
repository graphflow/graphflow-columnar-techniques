package ca.waterloo.dsg.graphflow.datachunk.vectors.node;

import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorIterator;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;

public class VectorNodeSequence extends Vector {

    private int nodeType;
    private int startNodeOffset;

    @Override
    public int getNodeType(int pos) {
        return nodeType;
    }

    @Override
    public int getNodeOffset(int pos) {
        return startNodeOffset + pos;
    }

    @Override
    public void setNodeType(int nodeType) {
        this.nodeType = nodeType;
    }

    @Override
    public void setNodeOffset(int nodeOffset) {
        this.startNodeOffset = nodeOffset;
    }

    @Override
    public VectorIterator getIterator() {
        return new VectorNodeSequenceIterator(this);
    }

    public static class VectorNodeSequenceIterator implements VectorIterator {

        private final VectorNodeSequence vector;
        private int pos;

        VectorNodeSequenceIterator(VectorNodeSequence vector) {
            this.vector = vector;
        }

        @Override
        public void init() {
            this.pos = 0;
        }

        @Override
        public int getNextNodeOffset() {
            return vector.startNodeOffset + pos;
        }

        @Override
        public void moveCursor() {
            pos++;
        }
    }
}
