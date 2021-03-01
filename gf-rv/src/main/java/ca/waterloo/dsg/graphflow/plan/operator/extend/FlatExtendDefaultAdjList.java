package ca.waterloo.dsg.graphflow.plan.operator.extend;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.iterators.DefaultAdjListSliceNodeAndRelIterator;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.RelVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public abstract class FlatExtendDefaultAdjList extends ExtendDefaultAdjList {

    DefaultAdjListSliceNodeAndRelIterator defaultAdjListSliceIterator;

    FlatExtendDefaultAdjList(AdjListDescriptor ald, Schema inSchema) {
        super(ald, inSchema);
    }

    @Override
    public void initFurther(Graph graph) {
        super.initFurther(graph);
        flatRelVal.setRelLabel(ald.getRelVariable().getLabel());
        defaultAdjListSliceIterator = DefaultAdjListSliceNodeAndRelIterator.make(graph,
            ald.getRelVariable().getLabel(), ald.getDirection(), fromNodeVal, flatNodeVal,
            flatRelVal);
    }

    @Override
    protected void setInputOutputTuples() {
        setInputTupleCopyOverToOutputTupleAndExtendBy(2);
        fromNodeVal = (NodeVal) inputTuple.get(inputTuple.getIdx(ald.getBoundNodeVariable()
            .getVariableName()));
        flatNodeVal = (NodeVal) ValueFactory.getFlatValueForDataType(ald.getToNodeVariable()
            .getVariableName(), DataType.NODE);
        outputTuple.set(this.inputTuple.numValues(), flatNodeVal);
        flatRelVal = (RelVal) ValueFactory.getFlatValueForDataType(ald.getRelVariable()
            .getVariableName(), DataType.RELATIONSHIP);
        outputTuple.set(this.inputTuple.numValues() + 1, flatRelVal);
        if (Direction.FORWARD == ald.getDirection()) {
            flatRelVal.setRelSrcNodeVal(fromNodeVal);
            flatRelVal.setRelDstNodeVal(flatNodeVal);
        } else {
            flatRelVal.setRelSrcNodeVal(flatNodeVal);
            flatRelVal.setRelDstNodeVal(fromNodeVal);
        }
    }

    public static class FlatExtendDefaultAdjListSingleType
        extends FlatExtendDefaultAdjList {

        public FlatExtendDefaultAdjListSingleType(AdjListDescriptor ald, Schema inSchema) {
            super(ald, inSchema);
        }

        @Override
        protected void extend() {
            defaultAdjListSliceIterator.init(defaultAdjListSlice);
            while (defaultAdjListSliceIterator.hasNextRel()) {
                defaultAdjListSliceIterator.nextRel();
                icost++;
                numOutTuples++;
                next.processNewTuple();
            }
        }
    }

    public static class FlatExtendDefaultAdjListMultiType
        extends FlatExtendDefaultAdjList {

        public FlatExtendDefaultAdjListMultiType(AdjListDescriptor ald, Schema inSchema) {
            super(ald, inSchema);
        }

        @Override
        protected void extend() {
            defaultAdjListSliceIterator.init(defaultAdjListSlice);
            while (defaultAdjListSliceIterator.hasNextRel()) {
                defaultAdjListSliceIterator.nextRel();
                icost++;
                if (flatNodeVal.getNodeType() == toType) {
                    numOutTuples++;
                    next.processNewTuple();
                }
            }
        }
    }
}
