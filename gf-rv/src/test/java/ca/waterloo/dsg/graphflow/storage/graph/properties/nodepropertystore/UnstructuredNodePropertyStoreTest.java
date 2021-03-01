package ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore;

import ca.waterloo.dsg.graphflow.util.DataLoader;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class UnstructuredNodePropertyStoreTest {

    @BeforeAll
    public static void load() {
        DataLoader.getDataset("railway");
    }

    @Test
    public void railwayGraphGetPropertyTest() throws IOException {
        var dataset = DataLoader.getDataset("railway");
        var propertyDescriptions = dataset.nodePropertyDescriptions;
        var graph = dataset.graph;
        var vertexStore = graph.getNodePropertyStore();
        var vertexData = DataLoader.getNodeData("railway");
        for (int i = 0; i < propertyDescriptions.length; i++) {
            var propertyDataTypes = propertyDescriptions[i].a;
            var propertyKeys = propertyDescriptions[i].b;
            for (var line : vertexData.get(i)) {
                var vertexId = Long.parseLong(line[0]);
                for (var j = 1; j < propertyDataTypes.length; j++) {
                    var vertexType = dataset.nodeIDMapping.getNodeType(vertexId);
                    var vertexOffset = dataset.nodeIDMapping.getNodeOffset(vertexId);
                    switch (propertyDataTypes[j]) {
                        case INT:
                            var actualInt = vertexStore.getInt(vertexType, vertexOffset,
                            propertyKeys[j]);
                            var expectedInt = line[j].length() == 0 ? DataType.NULL_INTEGER :
                                Integer.parseInt(line[j]);
                            Assertions.assertEquals(expectedInt, actualInt);
                            break;
                        case DOUBLE:
                            var actualDouble = vertexStore.getDouble(vertexType, vertexOffset,
                                propertyKeys[j]);
                            var expectedDouble = line[j].length() == 0 ? DataType.NULL_DOUBLE :
                                Double.parseDouble(line[j]);
                            System.out.println(Double.doubleToLongBits(expectedDouble));
                            System.out.println(Double.doubleToLongBits(actualDouble));
                                Assertions.assertEquals(expectedDouble, actualDouble, DataType.DELTA);
                            break;
                        case STRING:
                            var actualString = vertexStore.getString(vertexType, vertexOffset,
                                propertyKeys[j]);
                            var expectedString = line[j].length() == 0 ? null : line[j];
                            Assertions.assertTrue((expectedString == null && actualString == null)
                                || actualString.equals(expectedString));
                            break;
                        case BOOLEAN:
                            var actualBoolean = vertexStore.getBoolean(vertexType, vertexOffset,
                                propertyKeys[j]);
                            var expectedBoolean = line[j].length() != 0 &&
                                Boolean.parseBoolean(line[j]);
                            Assertions.assertEquals(expectedBoolean, actualBoolean);
                    }
                }
            }
        }
    }
}
