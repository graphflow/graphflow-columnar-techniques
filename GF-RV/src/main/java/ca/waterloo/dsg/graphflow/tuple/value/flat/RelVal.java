package ca.waterloo.dsg.graphflow.tuple.value.flat;

import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import lombok.Setter;

public class RelVal extends Value {

    @Getter @Setter private int relLabel;
    @Getter @Setter private NodeVal relSrcNodeVal;
    @Getter @Setter private NodeVal relDstNodeVal;
    @Getter @Setter private int relBucketOffset;

    public RelVal(String variableName) {
        this(variableName, GraphCatalog.ANY, DataType.NULL_INTEGER);
    }

    private RelVal(String variableName, int relLabel, int relBucketOffset) {
        super(variableName);
        this.relLabel = relLabel;
        this.relBucketOffset = relBucketOffset;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof RelVal &&
            this.relLabel == ((RelVal) other).relLabel &&
            this.relBucketOffset == ((RelVal) other).relBucketOffset;
    }

    @Override
    public DataType getDataType() {
        return DataType.RELATIONSHIP;
    }

    @Override
    public String getValAsStr() {
        return relLabel + ":(" + relSrcNodeVal.getValAsStr() + "):("  + relDstNodeVal.getValAsStr()
            + "):" + relBucketOffset;
    }

    @Override
    public Value copy() {
        return new RelVal(this.getVariableName(), this.relLabel, this.relBucketOffset);
    }

    public Value copy(NodeVal sNodeVal, NodeVal dNodeVal) {
        var newRelVal = new RelVal(this.getVariableName(), this.relLabel, this.relBucketOffset);
        newRelVal.setRelSrcNodeVal(sNodeVal);
        newRelVal.setRelDstNodeVal(dNodeVal);
        return newRelVal;
    }

    @Override
    public int hashCode() {
        long hash =  relLabel;
        hash = hash*31L + relSrcNodeVal.hashCode();
        hash = hash*31L + relDstNodeVal.hashCode();
        return (int) (hash*31L + relBucketOffset);
    }

    public void set(Value value) {
        if (value instanceof  RelVal) {
            this.relLabel = ((RelVal) value).relLabel;
            this.relBucketOffset = ((RelVal) value).relBucketOffset;
        } else {
            throw new UnsupportedOperationException(value.getClass().getCanonicalName() + " is not " +
                "RelVal.");
        }
    }

    public void setNodeVals(Tuple tuple, RelVal oldRelVal) {
        setRelSrcNodeVal((NodeVal) tuple.get(tuple.getIdx(oldRelVal.getRelSrcNodeVal()
            .getVariableName())));
        setRelDstNodeVal((NodeVal) tuple.get(tuple.getIdx(oldRelVal.getRelDstNodeVal()
            .getVariableName())));
    }
}
