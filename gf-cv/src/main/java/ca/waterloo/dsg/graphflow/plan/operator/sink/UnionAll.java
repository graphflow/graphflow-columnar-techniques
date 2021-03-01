package ca.waterloo.dsg.graphflow.plan.operator.sink;

import ca.waterloo.dsg.graphflow.tuple.Schema;

public class UnionAll extends AbstractUnion {

    public UnionAll(Schema inSchema) {
        super(inSchema);
        this.operatorName = "Union All: " + this.operatorName;
    }
}
