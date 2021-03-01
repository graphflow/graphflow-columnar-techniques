package ca.waterloo.dsg.graphflow.tuple.value.flat;

import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import lombok.Setter;

public class NodeVal extends Value {

    @Getter @Setter private int nodeType;
    @Getter @Setter private long nodeOffset;

    public int getInt() {
        return (int) nodeOffset;
    }
    public NodeVal(String variableName) {
        this(variableName, DataType.NULL_INTEGER, DataType.NULL_INTEGER);
    }

    public NodeVal(String variableName, int nodeType, long nodeOffset) {
        super(variableName);
        this.nodeType = nodeType;
        this.nodeOffset = nodeOffset;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof NodeVal && this.nodeType == ((NodeVal) other).nodeType
            && this.nodeOffset == ((NodeVal) other).nodeOffset;
    }

    @Override
    public DataType getDataType() {
        return DataType.NODE;
    }

    @Override
    public String getValAsStr() {
        return getNodeValue(nodeType, nodeOffset);
    }

    @Override
    public Value copy() {
        return new NodeVal(this.getVariableName(), this.nodeType, this.nodeOffset);
    }

    public static String getNodeValue(int nodeType, long nodeOffset) {
        return nodeType + ":" + nodeOffset;
    }

    @Override
    public int hashCode() {
        return (int) (((long) nodeType)*31L + nodeOffset);
    }

    public void set(Value value) {
        if (value instanceof  NodeVal) {
            this.nodeOffset = ((NodeVal) value).nodeOffset;
            this.nodeType = ((NodeVal) value).nodeType;
        } else {
            throw new UnsupportedOperationException(value.getClass().getCanonicalName() + " is not " +
                "NodeVal.");
        }
    }

    @Override
    public int compareTo(Value o) {
        return Long.compare(nodeOffset, o.getNodeOffset());
    }

}
