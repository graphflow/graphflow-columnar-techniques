package ca.waterloo.dsg.graphflow.datachunk.vectors.literal;

import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;

public class LiteralAsVector extends Vector {

    int valInt;
    double valDouble;
    String valString;
    boolean valBoolean;

    @Override
    public void set(int pos, int value) {
        this.valInt = value;
    }

    @Override
    public void set(int pos, double value) {
        this.valDouble = value;
    }

    @Override
    public void set(int pos, String value) {
        this.valString = value;
    }

    @Override
    public void set(int pos, boolean value) {
        this.valBoolean = value;
    }

    @Override
    public int getInt(int pos) {
        return valInt;
    }

    @Override
    public double getDouble(int pos) {
        return valDouble;
    }

    @Override
    public String getString(int pos) {
        return valString;
    }

    @Override
    public boolean getBoolean(int pos) {
        return valBoolean;
    }
}
