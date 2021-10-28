package ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.iterators;

import ca.waterloo.dsg.graphflow.storage.graph.BucketOffsetManager;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.value.flat.RelVal;

public class DefaultAdjListSliceRelIterator extends DefaultAdjListSliceIterator {

    public DefaultAdjListSliceRelIterator(RelVal relVal) {
        super();
        this.relVal = relVal;
    }

    protected void setDefaultInVals() {
        relVal.setRelBucketOffset(GraphCatalog.ANY);
    }

    public void nextRel() {
        intsArrayCursor++;
    }

    public static DefaultAdjListSliceRelIterator make(Graph graph, int label, RelVal relVal) {
        var catalog = graph.getGraphCatalog();
        if (!catalog.labelHasProperties(label)) {
            return new DefaultAdjListSliceRelIterator(relVal);
        } else if (!catalog.labelDirectionHasMultiplicityOne(label, Direction.FORWARD) &&
            !catalog.labelDirectionHasMultiplicityOne(label, Direction.BACKWARD)) {
            return new DefaultAdjListSliceBucketOffsetRelIterator(relVal);
        } else {
            return new DefaultAdjListSliceToValBucketOffsetRelIterator(relVal);
        }
    }

    public static class DefaultAdjListSliceToValBucketOffsetRelIterator
        extends DefaultAdjListSliceRelIterator {

        public DefaultAdjListSliceToValBucketOffsetRelIterator(RelVal relVal) {
            super(relVal);
        }

        public void nextRel() {
            var nodeOffset = defaultAdjListSlice.intsArray[intsArrayCursor++];
            relVal.setRelBucketOffset(BucketOffsetManager.getBucketOffset(nodeOffset));
        }
    }

    public static class DefaultAdjListSliceBucketOffsetRelIterator
        extends DefaultAdjListSliceRelIterator {

        public DefaultAdjListSliceBucketOffsetRelIterator(RelVal relVal) {
            super(relVal);
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

        public void nextRel() {
            intsArrayCursor++;
            relVal.setRelBucketOffset(defaultAdjListSlice.intsArray[intsArrayCursor++]);
        }
    }
}
