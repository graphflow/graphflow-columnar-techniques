package ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex;

public abstract class ColumnAdjListIndexTest {

    long numVertices = 1L << 12; // 2^12 = 4096
    Integer[] intVals = new Integer[]{0, 1, 1023, 1024, 4095};

    public ColumnAdjListIndexSingleType makeColumnAdjListIndexSingleTypeUncompressed() {
        var column = new ColumnAdjListIndexSingleType(0 /*vertexType*/, numVertices);
        for (var intVal : intVals) {
            column.setRel(intVal, intVal + 1 /*nbrOffset*/);
        }
        return column;
    }

    public ColumnAdjListIndexMultiType makeColumnAdjListIndexMultiTypeUncompressed() {
        var column = new ColumnAdjListIndexMultiType(0 /*vertexType*/, numVertices);
        for (var intVal : intVals) {
            column.setRel(intVal, intVal + 1 /*nbrOffset*/, (byte) (intVal % 2) /*nbrType*/);
        }
        return column;
    }
}
