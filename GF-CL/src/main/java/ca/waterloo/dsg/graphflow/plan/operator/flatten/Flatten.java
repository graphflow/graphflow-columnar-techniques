package ca.waterloo.dsg.graphflow.plan.operator.flatten;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;

public class Flatten extends Operator {

    private final String variableName;
    private DataChunk dataChunk;

    public Flatten(String variableName, Operator prev) {
        super(prev);
        this.variableName = variableName;
        this.operatorName = "Flatten: " + variableName;
    }

    @Override
    protected void initFurther(Graph graph) {
        dataChunk = dataChunks.getDataChunk(variableName);
        dataChunks.setAsFlat(variableName);
    }

    @Override
    public void processNewDataChunks() {
        var size = dataChunk.state.size;
        for (dataChunk.state.currPos = 0; dataChunk.state.currPos < size; dataChunk.state.currPos++) {
            next.processNewDataChunks();
        }
        dataChunk.state.currPos = -1;
    }
}
