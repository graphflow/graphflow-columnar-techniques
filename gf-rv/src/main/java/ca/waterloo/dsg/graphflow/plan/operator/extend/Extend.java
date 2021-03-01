package ca.waterloo.dsg.graphflow.plan.operator.extend;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.extend.FlatExtendDefaultAdjList.FlatExtendDefaultAdjListMultiType;
import ca.waterloo.dsg.graphflow.plan.operator.extend.FlatExtendDefaultAdjList.FlatExtendDefaultAdjListSingleType;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.RelVal;
import lombok.Getter;

import java.io.Serializable;

public abstract class Extend extends Operator implements Serializable {

    @Getter AdjListDescriptor ald;
    int toType;
    NodeVal fromNodeVal;
    NodeVal flatNodeVal;
    RelVal flatRelVal;

    public Extend(AdjListDescriptor ald, Schema inSchema) {
        super();
        this.toType = ald.getToNodeVariable().getType();
        this.ald = ald;
        var outSchema = inSchema.copy();
        outSchema.add(ald.getRelVariable().getVariableName(), ald.getRelVariable());
        outSchema.add(ald.getToNodeVariable().getVariableName(), ald.getToNodeVariable());
        this.outputTuple = new Tuple(outSchema);
        setOperatorName();
    }

    protected void setOperatorName() {
        var arrow = Direction.FORWARD == ald.getDirection() ? "->" : "<-";
        operatorName = String.format("%s: (%s)*%s(%s) using %s adjLists", getClass().getSimpleName(),
            ald.getBoundNodeVariable().getVariableName(), arrow,
            ald.getToNodeVariable().getVariableName(), ald.getDirection());
    }

    abstract protected void setInputOutputTuples();

    public static Extend makeFlat(AdjListDescriptor ald, Schema schema, GraphCatalog catalog) {
        var label = ald.getRelVariable().getLabel();
        if (catalog.labelDirectionHasSingleNbrType(label, ald.getDirection())) {
            return new FlatExtendDefaultAdjListSingleType(ald, schema);
        } else {
            return new FlatExtendDefaultAdjListMultiType(ald, schema);
        }
    }
}
