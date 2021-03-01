package ca.waterloo.dsg.graphflow.tuple.value;

import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.io.Serializable;

/**
 * Represents a data value, such as an integer, String, or double.
 */
public abstract class Value implements Serializable, Comparable<Value> {

    @Getter protected String variableName;

    public Value(String variableName) {
        this.variableName = variableName;
    }

    public int getInt() {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.getInt().");
    }

    public void setInt(int val) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.setInt(int).");
    }

    public String getString() {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.getString().");
    }

    public void setString(String val) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.setString(String).");
    }

    public boolean getBool() {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.getBool().");
    }

    public void setBool(boolean val) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.setBool(boolean).");
    }

    public double getDouble() {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.getDouble().");
    }

    public void setDouble(double val) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.setDouble().");
    }

    public int getNodeType() {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.getNodeType().");
    }

    public void setNodeType(int typeVal) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.setNodeType(int).");
    }

    public long getNodeOffset() {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.getNodeOffset().");
    }

    public void setNodeOffset(long offsetVal) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.setNodeOffset(long).");
    }

    public int getRelLabel() {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.getRelLabel().");
    }

    public void setRelLabel(int typeVal) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.setRelLabel(int).");
    }

    public void setRelSrcNodeVal(NodeVal nodeVal) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.setRelSrcNodeVal(NodeVal).");
    }

    public void setRelDstNodeVal(NodeVal nodeVal) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.setRelDstNodeVal(NodeVal).");
    }

    public int getRelBucketOffset() {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.getRelBucketOffset().");
    }

    public void setRelBucketOffset(int offsetVal) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.setRelBucketOffset(int).");
    }

    public abstract Value copy();

    @Override
    public abstract int hashCode();

    public abstract void set(Value value);

    public boolean isFactorized() {
        return false;
    }

    public abstract DataType getDataType();

    public abstract String getValAsStr();

    @Override
    public int compareTo(Value o) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement Value.compareTo(Value o).");
    }
}
