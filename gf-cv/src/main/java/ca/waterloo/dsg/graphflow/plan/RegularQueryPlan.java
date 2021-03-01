package ca.waterloo.dsg.graphflow.plan;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.sink.AbstractSink;
import ca.waterloo.dsg.graphflow.plan.operator.sink.AbstractUnion;
import ca.waterloo.dsg.graphflow.plan.operator.sink.Sink;
import ca.waterloo.dsg.graphflow.plan.operator.sink.Union;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Table;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import ca.waterloo.dsg.graphflow.util.container.Pair;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

public class RegularQueryPlan implements Serializable {

    private static final DecimalFormat formatter = new DecimalFormat("#.00");

    @Getter private List<SingleQueryPlan> singleQueryPlans;

    @Getter private double elapsedTimeMillis = 0;
    @Getter private Operator lastOperator;
    @Getter private long numOutputTuples = 0;

    @Getter @Setter double estimatedICost;
    @Getter @Setter double estimatedNumOutTuples;

    private boolean executed = false;
    private boolean initialized = false;

    public RegularQueryPlan() {

    }

    public void init(Graph graph) {
        singleQueryPlans.forEach(SingleQueryPlan::setNextPointers);
        lastOperator.init(graph);
        initialized = true;
    }

    public void execute() {
        if (!initialized) {
            throw new RuntimeException("Cannot execute the plan because plan is not initialized!");
        }
        var startTime = System.nanoTime();
        lastOperator.execute();
        elapsedTimeMillis = IOUtils.getTimeDiff(startTime);
        executed = true;
        numOutputTuples = lastOperator.getNumOutTuples();
    }

    public void appendSink(Sink sink) {
        lastOperator = sink;
        singleQueryPlans.forEach(singleQueryPlan -> singleQueryPlan.append(sink));
    }

    public void appendUnion(AbstractUnion abstractUnion) {
        lastOperator = abstractUnion;
        singleQueryPlans.forEach(singleQueryPlan ->
            ((AbstractUnion) lastOperator).addOperator(singleQueryPlan.getLastOperator()));
    }

    public void setSingleQueryPlans(List<SingleQueryPlan> singleQueryPlans) {
        this.singleQueryPlans = singleQueryPlans;
        singleQueryPlans.forEach(singleQueryPlan -> {
            this.estimatedICost += singleQueryPlan.getEstimatedICost();
            this.estimatedNumOutTuples += singleQueryPlan.getEstimatedNumOutTuples();
        });
    }

    public String outputAsString() {
        var sb = new StringBuilder();
        Pair<Long, Long> totalICostAndNumOutTuples = new Pair<>(0L, 0L);
        lastOperator.outputAsString(sb, initialized, totalICostAndNumOutTuples);
        var icost = totalICostAndNumOutTuples.a;
        var numIntermediateTuples = totalICostAndNumOutTuples.b;
        String execStats = "";
        if (executed) {
            execStats = (String.format("[elapsed:%f, #output:%d, #intermediate:%d, iCost:%d]: ",
                elapsedTimeMillis, numOutputTuples, numIntermediateTuples, icost));
        } else if (!initialized) {
            execStats = formatter.format(estimatedICost) + " | ";
        }
        return execStats + sb.toString();
    }

    public void setStoreTuples(boolean storeTuples) {
        if (lastOperator instanceof Union && !storeTuples) {
            throw new IllegalOperationException("Cannot set the storeTuples of Union to false!");
        }
        ((AbstractSink) this.lastOperator).setDummyCopy(false);
        ((AbstractSink) this.lastOperator).setStoreTuples(storeTuples);
    }

    public Table getOutputTable() {
        return ((AbstractSink) this.lastOperator).getOutputTable();
    }

    @Override
    public String toString() {
        return outputAsString();
    }
}

