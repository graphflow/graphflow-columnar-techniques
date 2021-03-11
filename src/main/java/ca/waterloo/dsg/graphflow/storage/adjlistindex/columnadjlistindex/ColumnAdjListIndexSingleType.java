package ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex;

import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnInteger;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class ColumnAdjListIndexSingleType extends ColumnInteger
    implements ColumnAdjListIndex {

    public ColumnAdjListIndexSingleType(int vertexType, int[][] slots) {
        super(vertexType, slots);
    }

    public ColumnAdjListIndexSingleType(int vertexType, long numVertices) {
        super(vertexType, numVertices);
    }

    // Used in Graph Loader

    @Override
    public void setRel(long vertexOffset, int neighbourOffset) {
        setProperty(vertexOffset, neighbourOffset);
    }

    // Used in Flat and Factorized operator implementation

    @Override
    public boolean hasRel(long vertexOffset) {
        return DataType.NULL_INTEGER != getProperty(vertexOffset);
    }

    // Used in VOperator implementation.

    public int getNodeOffset(long vertexOffset) {
        return getProperty(vertexOffset);
    }

    public int getNodeType(long vertexOffset) {
        throw new IllegalStateException("getNodeType is undefined for ST adjCols");
    }

    // SerDeser

    public static ColumnAdjListIndexSingleType deserialize(String directory) throws IOException,
        ClassNotFoundException {
        var inputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
            directory)));
        return new ColumnAdjListIndexSingleType(inputStream.readInt() /*vertexType*/,
            (int[][]) inputStream.readObject() /*slots*/);
    }
}
