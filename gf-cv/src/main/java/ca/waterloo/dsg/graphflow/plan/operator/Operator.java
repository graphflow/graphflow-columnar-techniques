package ca.waterloo.dsg.graphflow.plan.operator;

import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.util.container.Pair;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Base class for all database operators.
 */
public abstract class Operator implements Serializable {

    @Getter protected String operatorName;
    @Getter @Setter protected Operator next;
    @Getter @Setter protected Operator prev;

    @Getter protected Tuple inputTuple;
    @Getter protected Tuple outputTuple;

    @Getter protected long numOutTuples = 0;
    @Getter protected long icost = 0;

    protected Operator() {}

    public final void init(Graph graph) {
        if (null != prev) {
            prev.init(graph);
        }
        numOutTuples = 0;
        icost = 0;
        initFurther(graph);
    }

    protected abstract void initFurther(Graph graph);

    public abstract void processNewTuple();

    public void notifyAllDone() {
        if (null != next) {
            next.notifyAllDone();
        }
    }

    public void execute() {
        if (null != prev) {
            prev.execute();
        }
    }

    public Schema getOutSchema() {
        return outputTuple.getSchema();
    }

    protected final void setInputTupleCopyOverToOutputTupleAndExtendBy(int extensionSize) {
        this.inputTuple = null != prev ? prev.getOutputTuple() : new Tuple();
        outputTuple.copyTupleValuesAndExtendBy(this.inputTuple, extensionSize);
    }

    public void outputAsString(StringBuilder sb, boolean outputRuntimeData,
        Pair<Long, Long> summaryValues) {
        if (null != prev) {
            prev.outputAsString(sb, outputRuntimeData, summaryValues);
        }
        if (outputRuntimeData) {
            summaryValues.a += icost;
            summaryValues.b += numOutTuples;
            sb.append(String.format("[%s, iCost:%d, #:%d]-->", operatorName, icost, numOutTuples));
        } else {
            sb.append(this.operatorName + ", ");
        }
    }
}
