package ca.waterloo.dsg.graphflow.plan.operator.skip;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;

public class Skip extends Operator {

    private long numTuplesToSkip;

    private long numTuplesSkipped = 0;

    public Skip(long numTuplesToSkip, Schema inSchema) {
        this.numTuplesToSkip = numTuplesToSkip;
        this.outputTuple = new Tuple(inSchema.copy());
        operatorName = "SKIP " + this.numTuplesToSkip;
    }

    @Override
    public void initFurther(Graph graph) {
        setInputTupleCopyOverToOutputTupleAndExtendBy(0);
    }

    @Override
    public void processNewTuple() {
        if (numTuplesSkipped < numTuplesToSkip) {
            numTuplesSkipped++;
        } else {
            numOutTuples++;
            next.processNewTuple();
        }
    }
}
