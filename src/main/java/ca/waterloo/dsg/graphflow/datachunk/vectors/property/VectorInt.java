package ca.waterloo.dsg.graphflow.datachunk.vectors.property;

public class VectorInt extends Vector {

    protected int[] values;

    protected VectorInt(int capacity) {
        this.values = new int[capacity];
    }

    @Override
    public int getInt(int pos) {
        return values[pos];
    }

    @Override
    public int[] getInts() {
        return values;
    }

    @Override
    public void set(int pos, int value) {
        this.values[pos] = value;
    }

    @Override
    public void set(int[] values) {
        this.values = values;
    }
}
