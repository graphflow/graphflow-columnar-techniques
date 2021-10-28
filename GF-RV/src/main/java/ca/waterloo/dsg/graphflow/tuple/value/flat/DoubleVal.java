package ca.waterloo.dsg.graphflow.tuple.value.flat;

import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.text.DecimalFormat;

@SuppressWarnings("overrides")
public class DoubleVal extends Value {

    private double val;
    private static DecimalFormat df = new DecimalFormat("#.##########");

    public DoubleVal(String variableName, double val) {
        super(variableName);
        this.val = val;
    }

    public DoubleVal(double val) {
        this("_gFDouble", val);
    }

    @Override
    public double getDouble() { return val; }

    @Override
    public void setDouble(double val) { this.val = val; }

    @Override
    public DataType getDataType() {
        return DataType.DOUBLE;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof DoubleVal && df.format(this.val).equals(df.format(((DoubleVal) other).val));
    }

    @Override
    public String getValAsStr() {
        return "" + df.format(val);
    }

    @Override
    public Value copy() {
        return new DoubleVal(this.getVariableName(), this.val);
    }

    @Override
    public int hashCode() {
        return df.format(this.val).hashCode();
    }

    public void set(Value value) {
        if (value instanceof  DoubleVal) {
            this.val = ((DoubleVal) value).val;
        } else {
            throw new UnsupportedOperationException(value.getClass().getCanonicalName() + " is not " +
                "DoubleVal.");
        }
    }

    @Override
    public int compareTo(Value o) {
        return Double.compare(val, o.getDouble());
    }
}
