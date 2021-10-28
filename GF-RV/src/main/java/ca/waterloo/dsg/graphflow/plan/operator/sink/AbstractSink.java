package ca.waterloo.dsg.graphflow.plan.operator.sink;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Table;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.util.container.Pair;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

public abstract class AbstractSink extends Operator {

    @Setter protected boolean storeTuples = false;
    @Setter protected boolean dummyCopy = false;

    @Getter protected Table outputTable;
    protected int[] intValues;
    protected String[] strValues;
    protected int intP, strP;
    protected List<Integer> copyIntValIdxs, copyStrValIdxs;

    public AbstractSink(Schema inSchema) {
        this.outputTuple = new Tuple(inSchema.copy());
        this.operatorName = this.getOutSchema().getVariableNamesAsString();
    }

    @Override
    public void processNewTuple() {
        if (storeTuples) {
            outputTable.add(inputTuple);
        }
        numOutTuples++;
        icost++;
    }

    protected void addSinkOutputString(StringBuilder sb,
        boolean outputRuntimeData, Pair<Long, Long> summaryValues) {
        if (outputRuntimeData) {
            summaryValues.a += icost;
            summaryValues.b += numOutTuples;
            sb.append(String.format("[%s, iCost:%d, #:%d]", operatorName,
                icost, numOutTuples));
        } else {
            sb.append(this.operatorName);
        }
    }
}
