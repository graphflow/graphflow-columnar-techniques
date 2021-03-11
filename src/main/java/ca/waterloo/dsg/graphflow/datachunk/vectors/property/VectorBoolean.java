package ca.waterloo.dsg.graphflow.datachunk.vectors.property;

public class VectorBoolean extends Vector {

    protected boolean[] values;

    protected VectorBoolean(int capacity) {
        this.values = new boolean[capacity];
    }

    @Override
    public boolean getBoolean(int pos) {
        return values[pos];
    }

    @Override
    public boolean[] getBooleans() {
        return values;
    }

    @Override
    public void set(int pos, boolean value) {
        this.values[pos] = value;
    }

    @Override
    public void set(boolean[] values) {
        this.values = values;
    }
}
