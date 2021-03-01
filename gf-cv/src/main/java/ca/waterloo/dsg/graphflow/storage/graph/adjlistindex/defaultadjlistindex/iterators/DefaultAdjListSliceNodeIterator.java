package ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.iterators;

import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;

public abstract class DefaultAdjListSliceNodeIterator extends DefaultAdjListSliceIterator {

    public DefaultAdjListSliceNodeIterator(NodeVal nodeVal) {
        this.nodeVal = nodeVal;
    }

    public static DefaultAdjListSliceNodeIterator make(Graph graph, int label, Direction direction,
        NodeVal nodeVal) {
        var nbrTypes = graph.getGraphCatalog().getLabelToNbrTypeMapInDirection(direction).get(label);
        if (!graph.getGraphCatalog().labelHasProperties(label)) {
            return 1 == nbrTypes.size() ?
                new DefaultAdjListSliceSingleTypeNodeIterator(nodeVal, nbrTypes.get(0)) :
                new DefaultAdjListSliceMultiTypeNodeIterator(nodeVal);
        } else if (!graph.getGraphCatalog().labelDirectionHasMultiplicityOne(label, Direction.FORWARD) &&
            !graph.getGraphCatalog().labelDirectionHasMultiplicityOne(label, Direction.BACKWARD)) {
            return 1 == nbrTypes.size() ?
                new DefaultAdjListSliceSingleTypeBucketOffsetNodeIterator(nodeVal, nbrTypes.get(0)) :
                new DefaultAdjListSliceMultiTypeBucketOffsetNodeIterator(nodeVal);
        } else {
            return 1 == nbrTypes.size() ?
                new DefaultAdjListSliceSingleTypeNodeIterator(nodeVal, nbrTypes.get(0)) :
                new DefaultAdjListSliceMultiTypeNodeIterator(nodeVal);
        }
    }

    public static class DefaultAdjListSliceSingleTypeNodeIterator extends
        DefaultAdjListSliceNodeIterator {

        int singleNbrType;

        public DefaultAdjListSliceSingleTypeNodeIterator(NodeVal nodeVal, int singleNbrType) {
            super(nodeVal);
            this.singleNbrType = singleNbrType;
        }

        protected void setDefaultInVals() {
            nodeVal.setNodeType(singleNbrType);
        }

        public void nextRel() {
            nodeVal.setNodeOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
        }
    }

    public static class DefaultAdjListSliceSingleTypeBucketOffsetNodeIterator
        extends DefaultAdjListSliceSingleTypeNodeIterator {

        public DefaultAdjListSliceSingleTypeBucketOffsetNodeIterator(NodeVal nodeVal,
            int singleNbrType) {
            super(nodeVal, singleNbrType);
        }

        protected void setCursors() {
            intsArrayCursor = defaultAdjListSlice.relIdx +
                (defaultAdjListSlice.relsOffsetStart * 2);
            bytesArrayCursor = defaultAdjListSlice.relsOffsetStart;
            intsArrayLabelEndIdx = defaultAdjListSlice.relIdx +
                (defaultAdjListSlice.relsOffsetEnd * 2);
        }

        public int numRelsLeft() {
            return (intsArrayLabelEndIdx - intsArrayCursor) / 2;
        }

        public void skipNextRel() {
            intsArrayCursor += 2;
        }

        public void nextRel() {
            super.nextRel();
            intsArrayCursor++;
        }
    }

    public static class DefaultAdjListSliceMultiTypeNodeIterator extends
        DefaultAdjListSliceNodeIterator {

        public DefaultAdjListSliceMultiTypeNodeIterator(NodeVal nodeVal) {
            super(nodeVal);
        }

        protected void setDefaultInVals() { }

        public void nextRel() {
            nodeVal.setNodeOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
            nodeVal.setNodeType(defaultAdjListSlice.bytesArray[bytesArrayCursor++]);
        }
    }

    public static class DefaultAdjListSliceMultiTypeBucketOffsetNodeIterator
        extends DefaultAdjListSliceMultiTypeNodeIterator {

        public DefaultAdjListSliceMultiTypeBucketOffsetNodeIterator(NodeVal nodeVal) {
            super(nodeVal);
        }

        protected void setCursors() {
            intsArrayCursor = defaultAdjListSlice.relIdx +
                (defaultAdjListSlice.relsOffsetStart * 2);
            bytesArrayCursor = defaultAdjListSlice.relsOffsetStart;
            intsArrayLabelEndIdx = defaultAdjListSlice.relIdx +
                (defaultAdjListSlice.relsOffsetEnd * 2);
        }

        public int numRelsLeft() {
            return (intsArrayLabelEndIdx - intsArrayCursor) / 2;
        }

        public void skipNextRel() {
            intsArrayCursor += 2;
        }

        public void nextRel() {
            super.nextRel();
            intsArrayCursor++;
        }
    }
}
