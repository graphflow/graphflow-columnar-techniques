package ca.waterloo.dsg.graphflow.plan.operator.sink;

import ca.waterloo.dsg.graphflow.tuple.Schema;

public class Union extends AbstractUnion {

    public Union(Schema inSchema) {
        super(inSchema);
        this.operatorName = "Union: " + this.operatorName;
    }

    @Override
    public void notifyAllDone() {
        outputTable.removeDuplicates();
        numOutTuples = outputTable.getTuples().size();
        icost = numOutTuples;
    }
}
