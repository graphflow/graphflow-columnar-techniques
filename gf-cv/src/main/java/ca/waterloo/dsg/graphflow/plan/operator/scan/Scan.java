package ca.waterloo.dsg.graphflow.plan.operator.scan;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.io.Serializable;

public abstract class Scan extends Operator implements Serializable {

    @Getter String nodeName;
    protected int type;
    @Getter int outNodeIdx;

    Scan(NodeVariable nodeVariable) {
        super();
        this.nodeName = nodeVariable.getVariableName();
        this.type = nodeVariable.getType();
        var outSchema = new Schema();
        outSchema.add(nodeName, nodeVariable);
        this.outputTuple = new Tuple(outSchema);
    }

    @Override
    public void processNewTuple() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() +
            " does not support processNewTuple().");
    }

    protected void setInputAndOutputTuples() {
        setInputTupleCopyOverToOutputTupleAndExtendBy(1);
        outNodeIdx = this.inputTuple.numValues();
        this.outputTuple.set(outNodeIdx, ValueFactory.getFlatValueForDataType(nodeName,
            DataType.NODE));
    }
}
