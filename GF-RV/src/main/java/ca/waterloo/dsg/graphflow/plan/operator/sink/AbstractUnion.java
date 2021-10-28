package ca.waterloo.dsg.graphflow.plan.operator.sink;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Table;
import ca.waterloo.dsg.graphflow.util.container.Pair;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractUnion extends AbstractSink {

    public enum UnionType {
        UNION,
        UNION_ALL
    }

    private List<Operator> children;

    public AbstractUnion(Schema inSchema) {
        super(inSchema);
        children = new ArrayList<>();
    }

    @Override
    public void initFurther(Graph graph) {
        this.inputTuple = children.get(0).getOutputTuple();
        children.forEach(child -> {
            child.setNext(this);
            child.init(graph);
        });
        if (storeTuples) {
            this.outputTable = new Table(inputTuple);
        }
    }

    public void execute() {
        children.forEach(child -> {
            inputTuple = child.getOutputTuple();
            child.execute();
        });
    }

    public void addOperator(Operator operator) {
        children.add(operator);
    }

    @Override
    public void outputAsString(StringBuilder sb, boolean outputRuntimeData,
        Pair<Long, Long> summaryValues) {
        for (int i = 0; i < children.size(); ++i) {
            sb.append("(SQP" + (i + 1) + ")");
            children.get(i).outputAsString(sb, outputRuntimeData, summaryValues);
        }

        sb.append("(" + this.operatorName.split(":")[0] + ")");
        addSinkOutputString(sb, outputRuntimeData, summaryValues);
    }
}
