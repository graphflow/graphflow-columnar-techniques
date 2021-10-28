package ca.waterloo.dsg.graphflow.plan.operator.sink;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;

import java.io.Serializable;

public class SinkCountChunks extends Operator implements Serializable {

    String[] QVO;

    DataChunk[] dataChunksAsLists;

    public SinkCountChunks(String[] QVO, Operator prev) {
        super(prev);
        this.QVO = QVO;
    }

    @Override
    protected void initFurther(Graph graph) {
        dataChunksAsLists = dataChunks.getUnflatDataChunks();
    }

    @Override
    public void processNewDataChunks() {
        long numOutTuplesInChunks = 1;
        for (var dataChunk : dataChunksAsLists) {
            numOutTuplesInChunks *= dataChunk.state.size;
        }
        numOutTuples += numOutTuplesInChunks;
    }

    @Override
    public void reset() {
        prev.getDataChunks().reset();
        numOutTuples = 0;
        prev.reset();
    }
}
