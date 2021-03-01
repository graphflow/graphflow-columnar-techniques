package ca.waterloo.dsg.graphflow.tuple.value.flat;

import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public class IntVal extends Value {

    private int val;


    public IntVal(String variableName, int val) {
        super(variableName);
        this.val = val;
    }

    public IntVal(int val) {
        this("_gFInt", val);
    }

    @Override
    public int getInt() { return (int) val; }

    @Override
    public void setInt(int val) { this.val = val; }

    @Override
    public double getDouble() { return val; }

    @Override
    public DataType getDataType() {
        return DataType.INT;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof IntVal && this.val == ((IntVal) other).val;
    }

    @Override
    public String getValAsStr() {
        return "" + val;
    }

    @Override
    public Value copy() {
        return new IntVal(this.getVariableName(), this.val);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.val);
    }

    public void set(Value value) {
        if (value instanceof IntVal) {
            this.val = ((IntVal) value).val;
        } else {
            throw new UnsupportedOperationException(value.getClass().getCanonicalName() + " is not " +
                "IntVal.");
        }
    }

    @Override
    public int compareTo(Value o) {
        return Integer.compare(val, o.getInt());
    }
}
