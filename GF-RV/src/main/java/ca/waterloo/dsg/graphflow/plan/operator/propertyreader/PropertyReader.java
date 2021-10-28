package ca.waterloo.dsg.graphflow.plan.operator.propertyreader;

import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public abstract class PropertyReader extends Operator {

    private final String variableToRead;
    int variableToReadIdx;
    final int propertyKey;
    final DataType valueDataType;
    final String variableToWrite;
    int variableToWriteIdx;

    public PropertyReader(PropertyVariable propertyVariable, Schema inSchema) {
        this.variableToRead = propertyVariable.getNodeOrRelVariable().getVariableName();
        this.propertyKey = propertyVariable.getPropertyKey();
        this.valueDataType = propertyVariable.getDataType();
        this.variableToWrite = propertyVariable.getVariableName();
        var outSchema = inSchema.copy();
        outSchema.add(variableToWrite, propertyVariable);
        this.outputTuple = new Tuple(outSchema);
    }

    @Override
    public void initFurther(Graph graph) {
        setInputTupleCopyOverToOutputTupleAndExtendBy(1);
        this.variableToReadIdx = inputTuple.getIdx(variableToRead);
        this.variableToWriteIdx = inputTuple.numValues();
    }

    @Override
    public void processNewTuple() {
        readValues();
        numOutTuples++;
        next.processNewTuple();
    }

    abstract void readValues();
}
