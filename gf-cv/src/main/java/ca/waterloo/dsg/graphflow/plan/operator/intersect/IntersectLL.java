package ca.waterloo.dsg.graphflow.plan.operator.intersect;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.defaultadjlist.DefaultAdjListIndex;
import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.iterators.DefaultAdjListSlice;
import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.iterators.DefaultAdjListSliceNodeAndRelIterator;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.RelVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.io.Serializable;

public class IntersectLL extends Operator implements Serializable {

    @Getter AdjListDescriptor[] alds = new AdjListDescriptor[2];
    DefaultAdjListIndex[] defaultAdjListIndexes = new DefaultAdjListIndex[2];
    DefaultAdjListSlice[] defaultAdjListSlices = new DefaultAdjListSlice[2];
    int[] labelIdxs = new int[2];
    NodeVal[] fromNodeVals = new NodeVal[2];
    NodeVal flatNodeVal;
    NodeVal[] tmpFlatNodeVal = new NodeVal[2];
    RelVal[] flatRelVals = new RelVal[2];
    DefaultAdjListSliceNodeAndRelIterator[] defaultAdjListSliceIterators =
        new DefaultAdjListSliceNodeAndRelIterator[2];

    public IntersectLL(AdjListDescriptor ald1, AdjListDescriptor ald2, Schema inSchema) {
        super();
        this.alds[0] = ald1;
        this.alds[1] = ald2;
        var outSchema = inSchema.copy();
        outSchema.add(ald1.getRelVariable().getVariableName(), ald1.getRelVariable());
        outSchema.add(ald2.getRelVariable().getVariableName(), ald2.getRelVariable());
        outSchema.add(ald1.getToNodeVariable().getVariableName(), ald1.getToNodeVariable());
        this.outputTuple = new Tuple(outSchema);
        setOperatorName();
    }

    protected void setInputOutputTuples() {
        setInputTupleCopyOverToOutputTupleAndExtendBy(3);
        flatNodeVal = (NodeVal) ValueFactory.getFlatValueForDataType(alds[0].getToNodeVariable()
            .getVariableName(), DataType.NODE);
        outputTuple.set(this.inputTuple.numValues(), flatNodeVal);
        for (var i = 0; i < 2; i++) {
            tmpFlatNodeVal[i] =
                (NodeVal) ValueFactory.getFlatValueForDataType(alds[i].getToNodeVariable()
                    .getVariableName(), DataType.NODE);
            fromNodeVals[i] = (NodeVal) inputTuple.get(inputTuple.getIdx(
                alds[i].getBoundNodeVariable().getVariableName()));
            flatRelVals[i] = (RelVal) ValueFactory.getFlatValueForDataType(alds[i].getRelVariable()
                .getVariableName(), DataType.RELATIONSHIP);
            outputTuple.set(this.inputTuple.numValues() + 1 + i, flatRelVals[i]);
            if (Direction.FORWARD == alds[i].getDirection()) {
                flatRelVals[i].setRelSrcNodeVal(fromNodeVals[i]);
                flatRelVals[i].setRelDstNodeVal(flatNodeVal);
            } else {
                flatRelVals[i].setRelSrcNodeVal(flatNodeVal);
                flatRelVals[i].setRelDstNodeVal(fromNodeVals[i]);
            }
        }
    }

    @Override
    public void initFurther(Graph graph) {
        setInputOutputTuples();
        for (var i = 0; i < 2; i++) {
            defaultAdjListSlices[i] = DefaultAdjListSlice.make(graph.getGraphCatalog(),
                alds[i].getRelVariable().getLabel(), alds[i].getDirection());
            labelIdxs[i] = graph.getGraphCatalog()
                .getTypeToDefaultAdjListIndexLabelsMapInDirection(alds[i].getDirection())
                .get(alds[i].getBoundNodeVariable().getType())
                .indexOf(alds[i].getRelVariable().getLabel());
            defaultAdjListIndexes[i] = graph.getAdjListIndexes().getDefaultAdjListIndexForDirection(
                alds[i].getDirection(), alds[i].getBoundNodeVariable().getType(), labelIdxs[i]);
            flatRelVals[i].setRelLabel(alds[i].getRelVariable().getLabel());
            defaultAdjListSliceIterators[i] = DefaultAdjListSliceNodeAndRelIterator.make(graph,
                alds[i].getRelVariable().getLabel(), alds[i].getDirection(), fromNodeVals[i],
                tmpFlatNodeVal[i], flatRelVals[i]);
        }
    }

    protected void setOperatorName() {
        operatorName = String.format("%s:", getClass().getSimpleName());
        for (var i = 0; i < 2; i++) {
            var arrow = Direction.FORWARD == alds[0].getDirection() ? "->" : "<-";
            operatorName += String.format(" (%s)*%s(%s) using %s adjLists",
                alds[0].getBoundNodeVariable().getVariableName(), arrow,
                alds[0].getToNodeVariable().getVariableName(), alds[0].getDirection());
        }
    }

    @Override
    public void processNewTuple() {
        for (var i = 0; i < 2; i++) {
            defaultAdjListIndexes[i].fillAdjList(defaultAdjListSlices[i],
                fromNodeVals[i].getNodeOffset());
        }
        if (!defaultAdjListSlices[0].isEmpty() && !defaultAdjListSlices[1].isEmpty() ) {
            defaultAdjListSliceIterators[0].init(defaultAdjListSlices[0]);
            defaultAdjListSliceIterators[1].init(defaultAdjListSlices[1]);
            boolean[] populated = {false, false};
            while ((populated[0] || defaultAdjListSliceIterators[0].hasNextRel()) &&
                (populated[1] || defaultAdjListSliceIterators[1].hasNextRel())) {
                if (!populated[0]) {
                    defaultAdjListSliceIterators[0].nextRel();
                    populated[0] = true;
                }
                if (!populated[1]) {
                    defaultAdjListSliceIterators[1].nextRel();
                    populated[1] = true;
                }
                if (tmpFlatNodeVal[0].equals(tmpFlatNodeVal[1])) {
                    flatNodeVal.setNodeType(tmpFlatNodeVal[0].getNodeType());
                    flatNodeVal.setNodeOffset(tmpFlatNodeVal[0].getNodeOffset());
                    populated[0] = false;
                    populated[1] = false;
                    icost++;
                    numOutTuples++;
                    next.processNewTuple();
                } else if (tmpFlatNodeVal[0].getNodeType() < tmpFlatNodeVal[1].getNodeType() ||
                    ((tmpFlatNodeVal[0].getNodeType() == tmpFlatNodeVal[1].getNodeType()) &&
                        tmpFlatNodeVal[0].getNodeOffset() < tmpFlatNodeVal[1].getNodeOffset())) {
                    populated[0] = false;
                } else {
                    populated[1] = false;
                }
            }
        }
    }

    public static IntersectLL makeFlat(AdjListDescriptor ald1, AdjListDescriptor ald2,
        Schema schema, GraphCatalog catalog) {
        var label1 = ald1.getRelVariable().getLabel();
        var label2 = ald1.getRelVariable().getLabel();
        if (catalog.labelDirectionHasMultiplicityOne(label1, ald1.getDirection()) ||
            catalog.labelDirectionHasMultiplicityOne(label2, ald2.getDirection())) {
            throw new IllegalArgumentException("intersect do not work with col adjLists.");
        } else {
            if (catalog.labelDirectionHasSingleNbrType(label1, ald1.getDirection()) &&
                catalog.labelDirectionHasSingleNbrType(label2, ald2.getDirection())) {
                return new IntersectLL(ald1, ald2, schema);
            } else {
                throw new IllegalArgumentException("intersect do not work with mt def adjLists.");
            }
        }
    }
}