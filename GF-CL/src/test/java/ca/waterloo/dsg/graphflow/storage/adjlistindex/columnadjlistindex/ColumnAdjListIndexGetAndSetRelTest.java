package ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;

public class ColumnAdjListIndexGetAndSetRelTest extends ColumnAdjListIndexTest {

    @Test
    public void ColumnAdjListIndexSingleTypeUncompressedGetAndSetRelTest() {
        testColumnAdjListIndexSingleType(makeColumnAdjListIndexSingleTypeUncompressed());
    }

    private void testColumnAdjListIndexSingleType(ColumnAdjListIndex column) {
        var intValsSet = new HashSet<>(Arrays.asList(intVals));
        for (var intVal = 0; intVal < numVertices; intVal++) {
            if (intValsSet.contains(intVal)) {
                Assertions.assertTrue(column.hasRel(intVal));
            } else {
                Assertions.assertFalse(column.hasRel(intVal));
            }
        }
    }

    @Test
    public void ColumnAdjListIndexMultiTypeFixedTypeGetAndSetRelTest() {
        testColumnAdjListIndexMultiType(makeColumnAdjListIndexMultiTypeUncompressed());
    }

    private void testColumnAdjListIndexMultiType(ColumnAdjListIndex column) {
        var intValsSet = new HashSet<>(Arrays.asList(intVals));
        for (var intVal = 0; intVal < numVertices; intVal++) {
            if (intValsSet.contains(intVal)) {
                Assertions.assertTrue(column.hasRel(intVal));
            } else {
                Assertions.assertFalse(column.hasRel(intVal));
            }
        }
    }
}
