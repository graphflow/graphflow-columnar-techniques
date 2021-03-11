package ca.waterloo.dsg.graphflow.plan.operator.extend;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges.VectorAdjEdges;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges.VectorAdjEdgesFactory;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.DefaultAdjListIndex;
import lombok.Getter;

import java.io.Serializable;

import static ca.waterloo.dsg.graphflow.storage.GraphCatalog.ANY;

public class ExtendAdjLists extends Operator implements Serializable {

    protected final int typeFilter;
    final protected boolean resetSelector;

    protected DefaultAdjListIndex index;
    @Getter private final AdjListDescriptor ALD;

    protected Vector inVector;
    protected VectorAdjEdges outVector;

    public ExtendAdjLists(AdjListDescriptor ALD, int typeFilter, boolean resetSelector,
        Operator prev) {
        super(prev);
        this.ALD = ALD;
        this.resetSelector = resetSelector;
        this.typeFilter = typeFilter;
        this.operatorName = String.format("%s: (%s)%s(%s)", getClass().getSimpleName(),
            ALD.getBoundNodeVariable().getVariableName(), (Direction.FORWARD == ALD.getDirection() ?
                "->" : "<-"), ALD.getToNodeVariable().getVariableName());
    }

    @Override
    protected void initFurther(Graph graph) {
        var labelIdx = graph.getGraphCatalog()
                            .getTypeToDefaultAdjListIndexLabelsMapInDirection(ALD.getDirection())
                            .get(ALD.getBoundNodeVariable().getType())
                            .indexOf(ALD.getRelVariable().getLabel());
        index = graph.getAdjListIndexes().getDefaultAdjListIndexForDirection(ALD.getDirection(),
            ALD.getBoundNodeVariable().getType(), labelIdx);

        var boundVarName = ALD.getBoundNodeVariable().getVariableName();
        inVector = dataChunks.getValueVector(boundVarName);
        dataChunks.setAsFlat(boundVarName);

        var nbrVarName = ALD.getToNodeVariable().getVariableName();
        var relName = ALD.getRelVariable().getVariableName();
        var outDataChunkIdx = dataChunks.size();
        dataChunks.addVarToPosEntry(nbrVarName, outDataChunkIdx, 0 /* vectorPos */);
        dataChunks.addVarToPosEntry(relName, outDataChunkIdx, 0 /* vectorPos */);
        outVector = VectorAdjEdgesFactory.make(graph.getGraphCatalog(),
            ALD.getRelVariable().getLabel(), ALD.getDirection());
        var outDataChunk = new DataChunk();
        outDataChunk.append(outVector);
        dataChunks.append(outDataChunk);
    }

    @Override
    public void processNewDataChunks() {
        var boundNodeOffset = inVector.getNodeOffset(inVector.state.getCurrSelectedValuesPos());
        var adjListSize = index.readAdjList(outVector, boundNodeOffset);
        if (typeFilter != ANY && adjListSize > 0) {
            adjListSize = outVector.filter(typeFilter);
        }
        if (0 == adjListSize) return;

        var adjListSizeToCopy = adjListSize;
        var numFullVectors = adjListSize / Vector.DEFAULT_VECTOR_SIZE;
        if (numFullVectors > 0) {
            outVector.state.size = Vector.DEFAULT_VECTOR_SIZE;
            for (var i = 0; i < numFullVectors; i++) {
                if (resetSelector) {
                    outVector.state.resetSelector(Vector.DEFAULT_VECTOR_SIZE);
                    outVector.state.size = Vector.DEFAULT_VECTOR_SIZE;
                }
                next.processNewDataChunks();
                outVector.moveIntsArrayOffset(Vector.DEFAULT_VECTOR_SIZE);
                adjListSizeToCopy -= Vector.DEFAULT_VECTOR_SIZE;
            }
        }
        if (0 == adjListSizeToCopy) return;

        outVector.state.size = adjListSizeToCopy;
        if (resetSelector) {
            outVector.state.resetSelector(adjListSizeToCopy);
        }
        next.processNewDataChunks();
    }

    @Override
    public ExtendAdjLists copy() {
        return new ExtendAdjLists(ALD, typeFilter, resetSelector, prev.copy());
    }
}
