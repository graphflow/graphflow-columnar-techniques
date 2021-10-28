package ca.waterloo.dsg.graphflow.datachunk;

import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorState;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;

import java.util.ArrayList;
import java.util.List;

public class DataChunk {

    public VectorState state;
    public List<Vector> vectors;

    public DataChunk() {
        state = new VectorState();
        vectors = new ArrayList<>();
    }

    public DataChunk(int stateCapacity) {
        state = new VectorState(stateCapacity);
        state.size = stateCapacity;
        vectors = new ArrayList<>();
    }

    public Vector getValueVector(int pos) {
        return vectors.get(pos);
    }

    public int getNumValueVectors() {
        return vectors.size();
    }

    public void append(Vector vector) {
        vector.state = this.state;
        vectors.add(vector);
    }
}
