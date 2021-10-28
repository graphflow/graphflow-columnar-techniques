package ca.waterloo.dsg.graphflow.tuple.value.flat;

import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public class StringVal extends Value {
    private String val;

    public StringVal(String variableName, String val) {
        super(variableName);
        this.val = val;
    }

    public StringVal(String val) {
        this("_gFString", val);
    }

    @Override
    public String getString() {
        return val;
    }

    @Override
    public void setString(String val) {
        this.val = val;
    }

    @Override
    public DataType getDataType() {
        return DataType.STRING;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof StringVal && this.val.equals(((StringVal) other).val);
    }

    @Override
    public String getValAsStr() {
        return val;
    }

    @Override
    public Value copy() {
        return new StringVal(this.getVariableName(), this.val);
    }

    @Override
    public int hashCode() {
        return this.val.hashCode();
    }

    public void set(Value value) {
        if (value instanceof StringVal) {
            this.val = ((StringVal) value).val;
        } else {
            throw new UnsupportedOperationException(value.getClass().getCanonicalName() + " is not " +
                "StringVal.");
        }
    }

    @Override
    public int compareTo(Value o) {
        return val.compareTo(o.getString());
    }
}
