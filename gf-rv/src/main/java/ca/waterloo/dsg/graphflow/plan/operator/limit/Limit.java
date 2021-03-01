package ca.waterloo.dsg.graphflow.plan.operator.limit;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;

public class Limit extends Operator {

    private long numTuplesToLimit;

    private Operator firstOperator;

    public Limit(long numTuplesToLimit, Schema inSchema) {
        this.numTuplesToLimit = numTuplesToLimit;
        this.outputTuple = new Tuple(inSchema.copy());
        this.firstOperator = this;
        while (null != firstOperator.getPrev()) {
            firstOperator = firstOperator.getPrev();
        }
        operatorName = "LIMIT " + this.numTuplesToLimit;
    }

    @Override
    public void initFurther(Graph graph) {
        setInputTupleCopyOverToOutputTupleAndExtendBy(0);
    }

    @Override
    public void processNewTuple() {
        if (numOutTuples < numTuplesToLimit) {
            numOutTuples++;
            next.processNewTuple();
        } else {
            firstOperator.notifyAllDone();
        }
    }
}
