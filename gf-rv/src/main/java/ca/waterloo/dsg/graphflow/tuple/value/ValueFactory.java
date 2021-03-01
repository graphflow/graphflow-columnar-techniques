package ca.waterloo.dsg.graphflow.tuple.value;

import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.tuple.value.flat.BoolVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.DoubleVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.RelVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.StringVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public class ValueFactory {

    public static Value getFlatValueForDataType(String variableName, DataType dataType) {
        switch (dataType) {
            case INT:
                return new IntVal(variableName, DataType.NULL_INTEGER);
            case DOUBLE:
                return new DoubleVal(variableName, DataType.NULL_DOUBLE);
            case BOOLEAN:
                return new BoolVal(variableName, false);
            case STRING:
                return new StringVal(variableName, "");
            case NODE:
                return new NodeVal(variableName);
            case RELATIONSHIP:
                return new RelVal(variableName);
            default:
                throw new IllegalArgumentException("Unknown PGM Type: " + dataType.name() +
                    ". Cannot generate a flat Value object for it in the inputTuple.");
        }
    }
}
