package ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.iterators;

import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.RelVal;

import java.io.Serializable;

public abstract class DefaultAdjListSliceIterator implements Serializable {

    protected NodeVal fromNodeVal;
    protected NodeVal nodeVal;
    protected RelVal relVal;

    protected DefaultAdjListSlice defaultAdjListSlice;
    protected int bytesArrayCursor;
    protected int intsArrayCursor;
    protected int intsArrayLabelEndIdx;

    public void init(DefaultAdjListSlice defaultAdjListSlice) {
        this.defaultAdjListSlice = defaultAdjListSlice;
        setCursors();
        setDefaultInVals();
    }

    protected void setCursors() {
        intsArrayCursor = defaultAdjListSlice.relIdx + defaultAdjListSlice.relsOffsetStart;
        bytesArrayCursor = defaultAdjListSlice.relsOffsetStart;
        intsArrayLabelEndIdx = defaultAdjListSlice.relIdx + defaultAdjListSlice.relsOffsetEnd;
    }

    protected abstract void setDefaultInVals();

    public int numRelsLeft() {
        return intsArrayLabelEndIdx - intsArrayCursor;
    }

    public void skipNextRel() {
        intsArrayCursor++;
    }

    public boolean hasNextRel() {
        return intsArrayLabelEndIdx > intsArrayCursor ;
    }

    abstract public void nextRel();
}
