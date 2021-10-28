package ca.waterloo.dsg.graphflow.datachunk.vectors.property;

public class VectorDouble extends Vector {

    protected double[] values;

    protected VectorDouble(int capacity) {
        this.values = new double[capacity];
    }

    @Override
    public double getDouble(int pos) {
        return values[pos];
    }

    @Override
    public double[] getDoubles() {
        return values;
    }

    @Override
    public void set(int pos, double value) {
        this.values[pos] = value;
    }

    @Override
    public void set(double[] values) {
        this.values = values;
    }
}
