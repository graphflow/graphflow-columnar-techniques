package ca.waterloo.dsg.graphflow.plan.operator.scan;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.node.VectorNodeSequence;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ScanBlocking extends Operator implements Serializable {

    @Getter String nodeName;
    protected int type;

    protected Vector outVector;
    final boolean resetSelector;

    private int currNodeOffset;
    @Setter private MorselDesc morselDesc;

    public static class MorselDesc {

        public static final int MORSEL_SIZE = 256;

        AtomicInteger currNodeOffset;
        int numNodes;

        public MorselDesc() {
            this.currNodeOffset = new AtomicInteger(0);
        }
    }

    public ScanBlocking(NodeVariable nodeVariable, boolean resetSelector) {
        super(null /* prev */);
        this.nodeName = nodeVariable.getVariableName();
        this.type = nodeVariable.getType();
        this.operatorName = "Scan: " + nodeName + ".ID";
        this.resetSelector = resetSelector;
    }

    @Override
    public void initFurther(Graph graph) {
        morselDesc.numNodes = (int) graph.getNumNodesPerType()[type];
        this.dataChunks = new DataChunks();
        this.dataChunks.append(allocateOutVector());
        this.dataChunks.addVarToPosEntry(nodeName, 0 /* dataChunkPos*/, 0 /* vectorPos */);
    }

    protected DataChunk allocateOutVector() {
        outVector = new VectorNodeSequence();
        var dataChunk = new DataChunk(MorselDesc.MORSEL_SIZE); // Vector.DEFAULT_VECTOR_SIZE);
        dataChunk.append(outVector);
        return dataChunk;
    }

    @Override
    public void processNewDataChunks() {}

    @Override
    public void reset() {
        currNodeOffset = 0;
        morselDesc.currNodeOffset.set(0);
        outVector.state.size = MorselDesc.MORSEL_SIZE; // Vector.DEFAULT_VECTOR_SIZE;
    }

    @Override
    public void execute() {
        currNodeOffset = morselDesc.currNodeOffset.getAndAdd(MorselDesc.MORSEL_SIZE);
        while (currNodeOffset < morselDesc.numNodes) {
            if (currNodeOffset + MorselDesc.MORSEL_SIZE >= morselDesc.numNodes) {
                    // Vector.DEFAULT_VECTOR_SIZE >= morselDesc.numNodes) {
                outVector.state.size = morselDesc.numNodes - currNodeOffset;
            }
            outVector.setNodeOffset(currNodeOffset);
            if (resetSelector) {
                outVector.state.resetSelector(outVector.state.size);
            }
            next.processNewDataChunks();
            currNodeOffset = morselDesc.currNodeOffset.getAndAdd(MorselDesc.MORSEL_SIZE);
        }
        notifyAllDone();
    }
}
