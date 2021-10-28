package ca.waterloo.dsg.graphflow.tuple;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.SimpleVariable;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.RelVal;
import lombok.Getter;

import java.io.Serializable;
import java.util.Arrays;

public class Tuple implements Serializable {
    // TODO: We are keeping track of the dataType information in two places. Both in the Value
    //  objects themselves as well as in the Schema. We should keep it in only one place and in
    //  schema. This has been causing bugs in the code that are very difficult to detect.

    public static class FGroup {
        @Getter boolean[] rIdxs;
        @Getter int n;

        public void set(boolean[] r, int n) {
            this.rIdxs = r;
            this.n = n;
        }

        FGroup copy() {
            var n = new FGroup();
            n.rIdxs = rIdxs == null ? null : Arrays.copyOf(rIdxs, rIdxs.length);
            n.n = this.n;
            return n;
        }
    }

    private Value[] values;
    @Getter private Schema schema;

    public Tuple() {
        this.values = new Value[0];
        this.schema = new Schema();
    }

    public Tuple(Schema schema) {
        this.values = new Value[0];
        this.schema = schema;

    }

    public Tuple(Value[] values, Schema schema) {
        this.values = values;
        this.schema = schema;
    }

    public int numValues() {
        return values.length;
    }

    public Value get(int i) {
        return values[i];
    }

    // Warning: This method changes the tuple and it is upon the caller to ensure that the
    // schema associated with the tuple is also fixed correctly.
    public void set(int i, Value value) {
        values[i] = value;
    }

    public void append(Value value, Expression expression) {
        copyValuesAndExtendBy(this.values, 1);
        this.values[this.values.length - 1] = value;
        if (!schema.containsVarName(value.getVariableName())) {
            schema.add(value.getVariableName(), expression);
        }
    }

    /*This function is currently used only in Tests to support testing of tables and tuples. We
    explicitly create the SimpleVariable expression for the give Value having the consistent
    dataType, which is wrong. Currently, the above hack works because we only test for the
    dataType to determine if the Schema is same. This function should be removed once all the
    tests are corrected to use the above append() method.*/
    public void append(Value value) {
        copyValuesAndExtendBy(this.values, 1);
        this.values[this.values.length - 1] = value;
        if (!schema.containsVarName(value.getVariableName())) {
            schema.add(value.getVariableName(), new SimpleVariable(value.getVariableName(),
                value.getDataType()));
        }
    }

    public void copyTupleValuesAndExtendBy(Tuple tuple, int extensionSize) {
        copyValuesAndExtendBy(tuple.values, extensionSize);
    }

    private void copyValuesAndExtendBy(Value[] values, int extensionSize) {
        var extendedCopy = new Value[values.length + extensionSize];
        for (int i = 0; i < values.length; ++i) {
            extendedCopy[i] = values[i];
        }
        this.values = extendedCopy;
    }

    public Tuple deepCopy() {
        var values = new Value[numValues()];
        for (var i = 0; i < numValues(); i++) {
            if (!(get(i) instanceof RelVal)) {
                values[i] = get(i).copy();
            }
        }
        var newTuple = new Tuple(values, schema);
        for (var i = 0; i < this.values.length; i++) {
            if (get(i) instanceof RelVal) {
                var oldRelVal = (RelVal) get(i);
                var sNodeVal = newTuple.get(newTuple.getIdx(
                    oldRelVal.getRelSrcNodeVal().getVariableName()));
                var dNodeVal = newTuple.get(newTuple.getIdx(
                    oldRelVal.getRelDstNodeVal().getVariableName()));
                values[i] = oldRelVal.copy((NodeVal) sNodeVal, (NodeVal) dNodeVal);
            }
        }
        return newTuple;
}

    public int getIdx(String variableName) {
        for (int i = 0; i < values.length; ++i) {
            if (values[i] != null && variableName.equals(values[i].getVariableName())) {
                return i;
            }
        }
        throw new IllegalArgumentException("Could not find a variable with name: " + variableName
            + " in tuple.");
    }

    public String schemaAsStr() {
        String[] vals = new String[numValues()];
        for (int i = 0; i < numValues(); ++i) {
            var colName = values[i].getVariableName();
            vals[i] = colName + ": " + values[i].getDataType().name();
        }
        return String.format(getPrintFormat(), (Object[]) vals);
    }

    public boolean hasSameSchema(Tuple ot) {
        return this.schema.isSame(ot.schema);
    }

    public String getPrintFormat() {
        String format = "";
        for (int i = 0; i < numValues(); ++i) {
            format += "%-40.40s ";
            if (i < numValues() - 1) {
                format += " | ";
            }
        }
        return format;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        for (int i = 0; i < values.length; ++i) {
            hash = 31 * hash + values[i].hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object other) {
        if (!this.hasSameSchema((Tuple) other)) return false;
        for (int i = 0; i < values.length; ++i) {
            if (!values[i].equals(((Tuple) other).values[i])) {
                return false;
            }
        }
        return true;
    }
}
