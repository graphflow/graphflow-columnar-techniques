package ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex;

public interface ColumnAdjListIndex {

    // Used in Graph Loader

    default void setRel(long vertexOffset, int neighbourOffset) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement setEdge(long vertexOffset, int neighbourOffset).");
    }

    default void setRel(long vertexOffset, int neighbourOffset, byte neighbourType) {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " does not " +
            "implement setEdge(long vertexOffset, int neighbourOffset, byte neighbourType).");
    }

    // Used in Flat and Factorized operator implementation

    boolean hasRel(long vertexOffset);

    // Used in VOperator implementation.

    int getNodeOffset(long vertexOffset);

    int getNodeType(long vertexOffset);
}
