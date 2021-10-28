package ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column;

import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;

public class ColumnGetAndSetNodePropertyTest {

    private long numVertices = 1L << 12; // 2^12 = 4096
    private Integer[] intVals = new Integer[]{0, 1, 1023, 1024, 4095};
    private Boolean[] booleanVals = new Boolean[]{true, true, false, true, false};
    Double[] doubleVals = new Double[] {23.23, 34.34, 53.53, 78.78, 92.92};
    private String[] stringVals = new String[]{"Chomsky", "Foucault", "Derrida", "Montaigne",
        "Nietzsche"};

    private ColumnInteger makeColumnIntegerFixedLength() {
        var column = new ColumnInteger(0, numVertices);
        for (var intVal : intVals) {
            column.setProperty(intVal, intVal);
        }
        return column;
    }

    private ColumnDouble makeColumnDoubleFixedLength() {
        var column = new ColumnDouble(0, numVertices);
        for (var i = 0; i < intVals.length; i++) {
            column.setProperty(intVals[i], doubleVals[i]);
        }
        return column;
    }

    private ColumnBoolean makeColumnBoolean() {
        var column = new ColumnBoolean(0, numVertices);
        for (var i = 0; i < intVals.length; i++) {
            column.setProperty(intVals[i], booleanVals[i]);
        }
        return column;
    }

    private ColumnString makeColumnString() {
        var column = new ColumnString(0, numVertices);
        for (var i = 0; i < intVals.length; i++) {
            column.setProperty(intVals[i], stringVals[i].getBytes());
        }
        return column;
    }

    @Test
    public void IntegerColumnGetAndSetPropertyTest() {
        testValuesInIntegerColumn(makeColumnIntegerFixedLength());
    }

    private void testValuesInIntegerColumn(ColumnInteger column) {
        var intValsSet = new HashSet<>(Arrays.asList(intVals));
        for (var intVal = 0; intVal < numVertices; intVal++) {
            var val = column.getProperty(intVal);
            if (intValsSet.contains(intVal)) {
                Assertions.assertEquals(intVal, val);
            } else {
                Assertions.assertEquals(DataType.NULL_INTEGER, val);
            }
        }
    }

    @Test
    public void DoubleColumnGetAndSetPropertyTest() {
        testValuesInDoubleColumn(makeColumnDoubleFixedLength());
    }

    private void testValuesInDoubleColumn(ColumnDouble column) {
        var intValsList = Arrays.asList(intVals);
        var intValsSet = new HashSet<>(intValsList);
        for (var intVal = 0; intVal < numVertices; intVal++) {
            var val = column.getProperty(intVal);
            if (intValsSet.contains(intVal)) {
                Assertions.assertEquals(doubleVals[intValsList.indexOf(intVal)], val,
                    DataType.DELTA);
            } else {
                Assertions.assertEquals(DataType.NULL_DOUBLE, val, DataType.DELTA);
            }
        }
    }

    @Test
    public void BooleanColumnGetAndSetPropertyTest() {
        var column = makeColumnBoolean();
        var intValsList = Arrays.asList(intVals);
        var intValsSet = new HashSet<>(intValsList);
        for (var intVal = 0; intVal < numVertices; intVal++) {
            var val = column.getProperty(intVal);
            if (intValsSet.contains(intVal)) {
                Assertions.assertEquals(booleanVals[intValsList.indexOf(intVal)], val);
            } else {
                Assertions.assertFalse(val);
            }
        }
    }

    @Test
    public void StringColumnGetAndSetPropertyTest() {
        testValuesInStringColumn(makeColumnString());
    }

    private void testValuesInStringColumn(ColumnString column) {
        var intValsList = Arrays.asList(intVals);
        var intValsSet = new HashSet<>(intValsList);
        for (var intVal = 0; intVal < numVertices; intVal++) {
            var val = column.getProperty(intVal);
            if (intValsSet.contains(intVal)) {
                Assertions.assertEquals(stringVals[intValsList.indexOf(intVal)], val);
            } else {
                Assertions.assertNull(val);
            }
        }
    }
}
