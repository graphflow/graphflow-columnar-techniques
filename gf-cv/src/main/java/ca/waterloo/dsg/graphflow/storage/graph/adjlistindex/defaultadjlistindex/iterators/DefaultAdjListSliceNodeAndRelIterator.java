package ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.iterators;

import ca.waterloo.dsg.graphflow.storage.graph.BucketOffsetManager;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.RelVal;

public abstract class DefaultAdjListSliceNodeAndRelIterator extends DefaultAdjListSliceIterator {

    public DefaultAdjListSliceNodeAndRelIterator(NodeVal fromNodeVal, NodeVal nodeVal,
        RelVal relVal) {
        super();
        this.fromNodeVal = fromNodeVal;
        this.nodeVal = nodeVal;
        this.relVal = relVal;
    }

    protected void setDefaultInVals() {
        relVal.setRelBucketOffset(GraphCatalog.ANY);
    }

    public static DefaultAdjListSliceNodeAndRelIterator make(Graph graph, int label,
        Direction direction, NodeVal fromNodeVal, NodeVal nodeVal, RelVal relVal) {
        var catalog = graph.getGraphCatalog();
        var nbrTypes = graph.getGraphCatalog().getLabelToNbrTypeMapInDirection(direction)
            .get(label);
        if (!catalog.labelHasProperties(label)) {
            return 1 == nbrTypes.size() ?
                new DefaultAdjListSliceSingleTypeNodeAndRelIterator(fromNodeVal, nodeVal, relVal,
                    nbrTypes.get(0)) :
                new DefaultAdjListSliceMultiTypeNodeAndRelIterator(fromNodeVal, nodeVal, relVal);
        } else if (!catalog.labelDirectionHasMultiplicityOne(label, Direction.FORWARD) &&
            !catalog.labelDirectionHasMultiplicityOne(label, Direction.BACKWARD)) {
                return 1 == nbrTypes.size() ?
                    new DefaultAdjListSliceSingleTypeBucketOffsetNodeAndRelIterator(fromNodeVal,
                        nodeVal, relVal, nbrTypes.get(0)) :
                    new DefaultAdjListSliceMultiTypeBucketOffsetNodeAndRelIterator(fromNodeVal,
                        nodeVal, relVal);
        } else if ((catalog.labelDirectionHasMultiplicityOne(label, Direction.FORWARD) &&
            direction == Direction.FORWARD) ||
            ((catalog.labelDirectionHasMultiplicityOne(label, Direction.BACKWARD) &&
                direction == Direction.BACKWARD))) {
            return 1 == nbrTypes.size() ?
                new DefaultAdjListSliceSingleTypeFromValBucketOffsetNodeAndRelIterator(fromNodeVal,
                    nodeVal, relVal, nbrTypes.get(0)) :
                new DefaultAdjListSliceMultiTypeFromValBucketOffsetNodeAndRelIterator(fromNodeVal,
                    nodeVal, relVal);
        } else {
            return 1 == nbrTypes.size() ?
                new DefaultAdjListSliceSingleTypeToValBucketOffsetNodeAndRelIterator(fromNodeVal,
                    nodeVal, relVal, nbrTypes.get(0)) :
                new DefaultAdjListSliceMultiTypeToValBucketOffsetNodeAndRelIterator(fromNodeVal,
                    nodeVal, relVal);
        }
    }

    public static class DefaultAdjListSliceSingleTypeNodeAndRelIterator extends
        DefaultAdjListSliceNodeAndRelIterator {

        int singleNbrType;

        public DefaultAdjListSliceSingleTypeNodeAndRelIterator(NodeVal fnVal, NodeVal nodeVal,
            RelVal relVal,
            int singleNbrType) {
            super(fnVal, nodeVal, relVal);
            this.singleNbrType = singleNbrType;
        }

        protected void setDefaultInVals() {
            super.setDefaultInVals();
            nodeVal.setNodeType(singleNbrType);
        }

        public void nextRel() {
            nodeVal.setNodeOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
        }
    }

    public static class DefaultAdjListSliceSingleTypeToValBucketOffsetNodeAndRelIterator
        extends DefaultAdjListSliceSingleTypeNodeAndRelIterator {

        public DefaultAdjListSliceSingleTypeToValBucketOffsetNodeAndRelIterator(NodeVal fnodeval,
            NodeVal nodeVal,
            RelVal relVal, int singleNbrType) {
            super(fnodeval, nodeVal, relVal, singleNbrType);
        }

        public void nextRel() {
            nodeVal.setNodeOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
            relVal.setRelBucketOffset(BucketOffsetManager.getBucketOffset(
                nodeVal.getNodeOffset()));
        }
    }

    public static class DefaultAdjListSliceSingleTypeFromValBucketOffsetNodeAndRelIterator
        extends DefaultAdjListSliceSingleTypeNodeAndRelIterator {

        public DefaultAdjListSliceSingleTypeFromValBucketOffsetNodeAndRelIterator(NodeVal fnodeval,
            NodeVal nodeVal, RelVal relVal, int singleNbrType) {
            super(fnodeval, nodeVal, relVal, singleNbrType);
        }

        public void nextRel() {
            nodeVal.setNodeOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
            relVal.setRelBucketOffset(BucketOffsetManager.getBucketOffset(
                fromNodeVal.getNodeOffset()));
        }
    }


    public static class DefaultAdjListSliceSingleTypeBucketOffsetNodeAndRelIterator
        extends DefaultAdjListSliceSingleTypeNodeAndRelIterator {

        public DefaultAdjListSliceSingleTypeBucketOffsetNodeAndRelIterator(NodeVal fnodeval,
            NodeVal nodeVal, RelVal relVal, int singleNbrType) {
            super(fnodeval, nodeVal, relVal, singleNbrType);
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
            nodeVal.setNodeOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
            relVal.setRelBucketOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
        }
    }

    public static class DefaultAdjListSliceMultiTypeNodeAndRelIterator extends
        DefaultAdjListSliceNodeAndRelIterator {

        public DefaultAdjListSliceMultiTypeNodeAndRelIterator(NodeVal fnval, NodeVal nodeVal,
            RelVal relVal) {
            super(fnval, nodeVal, relVal);
        }

        public void nextRel() {
            nodeVal.setNodeOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
            nodeVal.setNodeType(defaultAdjListSlice.bytesArray[bytesArrayCursor++]);
        }
    }

    public static class DefaultAdjListSliceMultiTypeToValBucketOffsetNodeAndRelIterator
        extends DefaultAdjListSliceMultiTypeNodeAndRelIterator {

        public DefaultAdjListSliceMultiTypeToValBucketOffsetNodeAndRelIterator(NodeVal fnodeval,
            NodeVal nodeVal, RelVal relVal) {
            super(fnodeval, nodeVal, relVal);
        }

        public void nextRel() {
            nodeVal.setNodeOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
            nodeVal.setNodeType(defaultAdjListSlice.bytesArray[bytesArrayCursor++]);
            relVal.setRelBucketOffset(BucketOffsetManager.getBucketOffset(
                nodeVal.getNodeOffset()));
        }
    }

    public static class DefaultAdjListSliceMultiTypeFromValBucketOffsetNodeAndRelIterator
        extends DefaultAdjListSliceMultiTypeNodeAndRelIterator {

        public DefaultAdjListSliceMultiTypeFromValBucketOffsetNodeAndRelIterator(NodeVal fnodeval,
            NodeVal nodeVal, RelVal relVal) {
            super(fnodeval, nodeVal, relVal);
        }

        public void nextRel() {
            nodeVal.setNodeOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
            nodeVal.setNodeType(defaultAdjListSlice.bytesArray[bytesArrayCursor++]);
            relVal.setRelBucketOffset(BucketOffsetManager.getBucketOffset(
                fromNodeVal.getNodeOffset()));
        }
    }

    public static class DefaultAdjListSliceMultiTypeBucketOffsetNodeAndRelIterator
        extends DefaultAdjListSliceMultiTypeNodeAndRelIterator {

        public DefaultAdjListSliceMultiTypeBucketOffsetNodeAndRelIterator(NodeVal fnodeval,
            NodeVal nodeVal, RelVal relVal) {
            super(fnodeval, nodeVal, relVal);
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
            nodeVal.setNodeOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
            nodeVal.setNodeType(defaultAdjListSlice.bytesArray[bytesArrayCursor++]);
            relVal.setRelBucketOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
        }
    }
}
