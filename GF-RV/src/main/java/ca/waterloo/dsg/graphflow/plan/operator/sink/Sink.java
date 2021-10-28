package ca.waterloo.dsg.graphflow.plan.operator.sink;

import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Table;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.StringVal;
import ca.waterloo.dsg.graphflow.util.container.Pair;

import java.util.ArrayList;

public class Sink extends AbstractSink {

    public Sink(Schema inSchema) {
        super(inSchema);
        this.operatorName = "Sink: " + this.operatorName;
    }

    @Override
    public void initFurther(Graph graph) {
        this.inputTuple = prev.getOutputTuple();
        if (storeTuples) {
            this.outputTable = new Table(inputTuple);
        }
        if (dummyCopy) {
            copyIntValIdxs = new ArrayList<>();
            copyStrValIdxs = new ArrayList<>();
            var num = inputTuple.numValues();
            for (var i = 0; i < num; i++) {
                var val = inputTuple.get(i);
                if (val instanceof IntVal || val instanceof NodeVal) {
                    copyIntValIdxs.add(i);
                } else if (val instanceof StringVal) {
                    copyStrValIdxs.add(i);
                }
            }
            intValues = new int[100000];
            strValues = new String[100000];
            intP = 0;
            strP = 0;
        }
    }

    /**
     * This method invokes the previous operator till it gets to one of the ScanDebug operators.
     */
    public void execute() {
        prev.execute();
    }

    @Override
    public void outputAsString(StringBuilder sb, boolean outputRuntimeData,
        Pair<Long, Long> summaryValues) {
        if (null != prev) {
            prev.outputAsString(sb, outputRuntimeData, summaryValues);
        }
        addSinkOutputString(sb, outputRuntimeData, summaryValues);
    }
}
