package ca.waterloo.dsg.graphflow.datachunk.vectors;

public interface VectorIterator {

    void init();
    void moveCursor();
    int getNextNodeOffset();
    default int getNextRelBucketOffset() { throw new UnsupportedOperationException(); }
}
