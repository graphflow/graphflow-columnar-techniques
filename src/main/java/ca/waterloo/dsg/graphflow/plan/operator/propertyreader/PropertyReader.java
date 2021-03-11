package ca.waterloo.dsg.graphflow.plan.operator.propertyreader;

import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorIterator;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;

public abstract class PropertyReader extends Operator {

    protected PropertyVariable variable;
    protected Vector inVector;
    protected Vector outVector;
    protected VectorIterator it;

    public PropertyReader(PropertyVariable variable, Operator prev) {
        super(prev);
        this.variable = variable;
        this.operatorName = "Scan: " + variable.getVariableName();
    }

    @Override
    protected void initFurther(Graph graph) {
        var nodeOrRelVarName = variable.getNodeOrRelVariable().getVariableName();
        var dataChunk = dataChunks.getDataChunk(nodeOrRelVarName);
        inVector = dataChunks.getValueVector(nodeOrRelVarName);
        it = inVector.getIterator();
        allocateOutVector();
        dataChunks.addVarToPosEntry(variable.getVariableName(), dataChunks.getDataChunkPos(
            nodeOrRelVarName), dataChunk.getNumValueVectors());
        dataChunk.append(outVector);
    }

    protected void allocateOutVector() {
        outVector = Vector.make(variable.getDataType());
    }

    @Override
    public void processNewDataChunks() {
        readValues();
        next.processNewDataChunks();
    }

    protected abstract void readValues();
}
