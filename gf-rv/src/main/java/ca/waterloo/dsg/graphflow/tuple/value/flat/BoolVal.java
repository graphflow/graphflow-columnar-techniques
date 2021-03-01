package ca.waterloo.dsg.graphflow.tuple.value.flat;

import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

@SuppressWarnings("overrides")
public class BoolVal extends Value {

    public static BoolVal TRUE_BOOL_VAL = new BoolVal(true);
    public static BoolVal FALSE_BOOL_VAL = new BoolVal(false);

    private boolean val;

    public BoolVal(String variableName, boolean val) {
        super(variableName);
        this.val = val;
    }

    public BoolVal(boolean val) {
        this("_gFBool", val);
    }

    @Override
    public boolean getBool() {
        return val;
    }

    @Override
    public void setBool(boolean val) {
        this.val = val;
    }

    @Override
    public DataType getDataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof BoolVal && this.val == ((BoolVal) other).val;
    }

    @Override
    public String getValAsStr() {
        return "" + val;
    }

    @Override
    public Value copy() {
        return new BoolVal(this.getVariableName(), this.val);
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(this.val);
    }

    public void set(Value value) {
        if (value instanceof BoolVal) {
            this.val = ((BoolVal) value).val;
        } else {
            throw new UnsupportedOperationException(value.getClass().getCanonicalName() + " is not " +
                "BoolVal.");
        }
    }

    @Override
    public int compareTo(Value o) {
        return Boolean.compare(val, o.getBool());
    }
}
