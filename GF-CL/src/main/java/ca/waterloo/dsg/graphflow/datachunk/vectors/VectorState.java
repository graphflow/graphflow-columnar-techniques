package ca.waterloo.dsg.graphflow.datachunk.vectors;

import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;

import java.util.stream.IntStream;

public class VectorState {

    static final int[] INITIAL_SELECTOR = IntStream.range(0, 1024).toArray();

    public int size = 0;
    public int currPos = -1;
    public int[] selectedValuesPos;

    public VectorState() {
        this(Vector.DEFAULT_VECTOR_SIZE);
    }

    public VectorState(int capacity) {
        selectedValuesPos = new int[capacity];
        resetSelector(capacity);
    }

    public VectorState(int capacity, int currPos, int size) {
        selectedValuesPos = new int[capacity];
        resetSelector(capacity);
        this.currPos = currPos;
        this.size = size;
    }

    public static VectorState getFlatVectorState() {
        return new VectorState(1 /* single item */, 0 /* currPos */, 1 /* size */);
    }

    public int getCurrSelectedValuesPos() { return selectedValuesPos[currPos]; }

    public boolean isFlat() { return currPos != -1; }

    public void resetSelector(int length) {
        System.arraycopy(INITIAL_SELECTOR, 0, selectedValuesPos, 0, length);
    }
}
