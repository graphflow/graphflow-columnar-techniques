package ca.waterloo.dsg.graphflow.plan.operator.extend;

import ca.waterloo.dsg.graphflow.datachunk.vectors.adjcols.VectorAdjCols;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjcols.VectorAdjCols.VectorAdjColsCopied;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.sink.SinkCountChunks;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex.ColumnAdjListIndex;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import static ca.waterloo.dsg.graphflow.storage.GraphCatalog.ANY;

public class ExtendColumn extends Operator {

    protected ColumnAdjListIndex index;
    @Getter protected final AdjListDescriptor ALD;

    protected Vector inVector;
    protected VectorAdjColsCopied outVector;

    protected int typeFilter;

    public ExtendColumn(AdjListDescriptor ALD, int typeFilter, Operator prev) {
        super(prev);
        this.ALD = ALD;
        this.typeFilter = typeFilter;
        var boundVarName = ALD.getBoundNodeVariable().getVariableName();
        var toVarName = ALD.getToNodeVariable().getVariableName();
        var arrow = Direction.FORWARD == ALD.getDirection() ? "->" : "<-";
        this.operatorName = String.format("%s: (%s)%s(%s)",
            getClass().getSimpleName(), boundVarName, arrow, toVarName);
    }

    @Override
    protected void initFurther(Graph graph) {
        var direction = ALD.getDirection();
        var labelIdx = graph.getGraphCatalog()
            .getTypeToColumnAdjListIndexLabelsMapInDirection(direction)
            .get(ALD.getBoundNodeVariable().getType())
            .indexOf(ALD.getRelVariable().getLabel());
        index = graph.getAdjListIndexes().getColumnAdjListIndexForDirection(direction,
            ALD.getBoundNodeVariable().getType(), labelIdx);

        var boundVarName = ALD.getBoundNodeVariable().getVariableName();
        var toVarName = ALD.getToNodeVariable().getVariableName();
        var relVarName = ALD.getRelVariable().getVariableName();
        var dataChunk = dataChunks.getDataChunk(boundVarName);
        inVector = dataChunks.getValueVector(boundVarName);

        var dataChunkPos = dataChunks.getDataChunkPos(boundVarName);
        var valueVectorPos = dataChunk.getNumValueVectors();
        dataChunks.addVarToPosEntry(toVarName, dataChunkPos, valueVectorPos);
        dataChunks.addVarToPosEntry(relVarName, dataChunkPos, valueVectorPos);
        allocateOutVector(graph, (int) graph.getNumRels());
        dataChunk.append(outVector);
    }

    @Override
    public void processNewDataChunks() {
        if (inVector.state.isFlat()) {
            var pos = inVector.state.getCurrSelectedValuesPos();
            var nbrOffset = index.getNodeOffset(inVector.getNodeOffset(pos));
            if (DataType.NULL_INTEGER == nbrOffset) {
                return;
            }
            outVector.setNodeOffset(nbrOffset, pos);
        } else {
            var numSelectedValues = 0;
            for (var i = 0; i < inVector.state.size; i++) {
                int boundOffset;
                int nbrOffset;
                var readPos = inVector.state.selectedValuesPos[i];
                boundOffset = inVector.getNodeOffset(readPos);
                nbrOffset = index.getNodeOffset(boundOffset);
                if (DataType.NULL_INTEGER != nbrOffset &&
                    (ANY == typeFilter || index.getNodeType(nbrOffset) == typeFilter)) {
                    outVector.setNodeOffset(nbrOffset, readPos);
                    outVector.state.selectedValuesPos[numSelectedValues++] = readPos;
                }
            }
            if (0 == numSelectedValues) return;
            outVector.state.size = numSelectedValues;
        }
        next.processNewDataChunks();
    }

    protected void allocateOutVector(Graph graph, int capacity) {
        outVector = (VectorAdjColsCopied) VectorAdjCols.make(graph.getGraphCatalog(),
            ALD.getRelVariable().getLabel(), ALD.getDirection(), capacity);
    }

    @Override
    public ExtendColumn copy() {
        return new ExtendColumn(ALD, typeFilter, prev.copy());
    }
}
