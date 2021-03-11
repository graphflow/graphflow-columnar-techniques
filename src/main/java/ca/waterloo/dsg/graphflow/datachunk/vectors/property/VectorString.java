package ca.waterloo.dsg.graphflow.datachunk.vectors.property;

public class VectorString extends Vector {

    protected String[] values;

    protected VectorString(int capacity) {
        this.values = new String[capacity];
    }

    @Override
    public String getString(int pos) {
        return values[pos];
    }

    @Override
    public String[] getStrings() {
        return values;
    }

    @Override
    public void set(int pos, String value) {
        this.values[pos] = value;
    }

    @Override
    public void set(String[] values) {
        this.values = values;
    }
}
