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
import java.util.concurrent.locks.ReentrantLock;

public class ScanBlocking extends Operator implements Serializable {

    @Getter String nodeName;
    protected int type;

    protected Vector outVector;
    final boolean resetSelector;

    private int currNodeOffset;
    private final MorselDesc morselDesc;

    public static class MorselDesc {
        int currNodeOffset;
        int highestOffset;
        ReentrantLock lock;

        public MorselDesc(int highestOffset) {
            this.highestOffset = highestOffset;
            this.currNodeOffset = 0;
            lock = new ReentrantLock();
        }
    }

    public ScanBlocking(NodeVariable nodeVariable, boolean resetSelector, MorselDesc morselDesc) {
        super(null /* prev */);
        this.morselDesc = morselDesc;
        this.nodeName = nodeVariable.getVariableName();
        this.type = nodeVariable.getType();
        this.operatorName = "Scan: " + nodeName + ".ID";
        this.resetSelector = resetSelector;
    }

    @Override
    public void initFurther(Graph graph) {
        // var highestOffset = graph.getNumNodesPerType()[type] - 1;
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
        outVector.state.size = outVector.state.size = Vector.DEFAULT_VECTOR_SIZE;
    }

    @Override
    public void execute() {
        grabMorsel();
        while(currNodeOffset < morselDesc.highestOffset) {
            if (morselDesc.currNodeOffset + Vector.DEFAULT_VECTOR_SIZE > morselDesc.highestOffset) {
                outVector.state.size = outVector.state.size =
                    morselDesc.highestOffset - morselDesc.currNodeOffset;
            }
            outVector.setNodeOffset(currNodeOffset);
            next.processNewDataChunks();
            grabMorsel();
        }

        /* this.numFullVectors = (int) highestOffset / Vector.DEFAULT_VECTOR_SIZE;
           this.finalOffset = numFullVectors * Vector.DEFAULT_VECTOR_SIZE;

        var currNodeOffset = 0;
        for (var i = 0; i < numFullVectors; i++) {
            outVector.setNodeOffset(currNodeOffset);
            if (resetSelector) {
                outVector.state.resetSelector(Vector.DEFAULT_VECTOR_SIZE);
                outVector.state.numSelectedValues = Vector.DEFAULT_VECTOR_SIZE;
            }
            next.processNewDataChunks();
            currNodeOffset += Vector.DEFAULT_VECTOR_SIZE;
        }
        outVector.setNodeOffset(finalOffset);
        outVector.state.size = finalSize;
        outVector.state.numSelectedValues = finalSize;
        if (resetSelector) {
            outVector.state.resetSelector(finalSize);
        }                                                                            */

        notifyAllDone();
    }

    private void grabMorsel() {
        morselDesc.lock.lock();
        currNodeOffset = morselDesc.currNodeOffset;
        morselDesc.currNodeOffset += Vector.DEFAULT_VECTOR_SIZE;
        morselDesc.lock.unlock();
    }
}
