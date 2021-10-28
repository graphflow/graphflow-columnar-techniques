package ca.waterloo.dsg.graphflow.plan.operator.scan;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.node.VectorNodeSequence;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;
import lombok.Getter;

import java.io.Serializable;

public class Scan extends Operator implements Serializable {

    @Getter String nodeName;
    protected int type;

    protected Vector outVector;
    protected int numFullVectors;
    protected int finalOffset;
    protected int finalSize;
    final boolean resetSelector;

    public Scan(NodeVariable nodeVariable, boolean resetSelector) {
        super(null /* prev */);
        this.nodeName = nodeVariable.getVariableName();
        this.type = nodeVariable.getType();
        this.operatorName = "Scan: " + nodeName + ".ID";
        this.resetSelector = resetSelector;
    }

    @Override
    public void initFurther(Graph graph) {
        var highestOffset = graph.getNumNodesPerType()[type] - 1;
        this.numFullVectors = (int) highestOffset / Vector.DEFAULT_VECTOR_SIZE;
        this.finalOffset = numFullVectors * Vector.DEFAULT_VECTOR_SIZE;
        this.finalSize = (int) (highestOffset - finalOffset) + 1;
        this.dataChunks = new DataChunks();
        this.dataChunks.append(allocateOutVector());
        this.dataChunks.addVarToPosEntry(nodeName, 0 /* dataChunkPos*/, 0 /* vectorPos */);
    }

    protected DataChunk allocateOutVector() {
        outVector = new VectorNodeSequence();
        var dataChunk = new DataChunk(Vector.DEFAULT_VECTOR_SIZE);
        dataChunk.append(outVector);
        return dataChunk;
    }

    @Override
    public void processNewDataChunks() {}

    @Override
    public void reset() {
        outVector.state.size = Vector.DEFAULT_VECTOR_SIZE;
    }

    @Override
    public void execute() {
        var currNodeOffset = 0;
        for (var i = 0; i < numFullVectors; i++) {
            outVector.setNodeOffset(currNodeOffset);
            if (resetSelector) {
                outVector.state.resetSelector(Vector.DEFAULT_VECTOR_SIZE);
                outVector.state.size = Vector.DEFAULT_VECTOR_SIZE;
            }
            next.processNewDataChunks();
            currNodeOffset += Vector.DEFAULT_VECTOR_SIZE;
        }
        outVector.setNodeOffset(finalOffset);
        outVector.state.size = finalSize;
        if (resetSelector) {
            outVector.state.resetSelector(finalSize);
        }
        next.processNewDataChunks();
        notifyAllDone();
    }
}
