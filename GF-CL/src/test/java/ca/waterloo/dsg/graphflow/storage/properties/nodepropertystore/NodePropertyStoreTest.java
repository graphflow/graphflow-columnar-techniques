package ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore;

import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnBoolean;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnDouble;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnInteger;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnString;
import ca.waterloo.dsg.graphflow.util.DataLoader;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class NodePropertyStoreTest {

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
                            var actualInt = ((ColumnInteger) vertexStore.getColumn(vertexType,
                                propertyKeys[j])).getProperty(vertexOffset);
                            var expectedInt = line[j].length() == 0 ? DataType.NULL_INTEGER :
                                Integer.parseInt(line[j]);
                            Assertions.assertEquals(expectedInt, actualInt);
                            break;
                        case DOUBLE:
                            var actualDouble = ((ColumnDouble) vertexStore.getColumn(vertexType,
                                propertyKeys[j])).getProperty(vertexOffset);
                            var expectedDouble = line[j].length() == 0 ? DataType.NULL_DOUBLE :
                                Double.parseDouble(line[j]);
                            Assertions.assertEquals(expectedDouble, actualDouble, DataType.DELTA);
                            break;
                        case STRING:
                            var actualString = ((ColumnString) vertexStore.getColumn(vertexType,
                                propertyKeys[j])).getProperty(vertexOffset);
                            var expectedString = line[j].length() == 0 ? null : line[j];
                            Assertions.assertTrue((expectedString == null && actualString == null)
                                || actualString.equals(expectedString));
                            break;
                        case BOOLEAN:
                            var actualBoolean = ((ColumnBoolean) vertexStore.getColumn(vertexType,
                                propertyKeys[j])).getProperty(vertexOffset);
                            var expectedBoolean = line[j].length() != 0 &&
                                Boolean.parseBoolean(line[j]);
                            Assertions.assertEquals(expectedBoolean, actualBoolean);
                    }
                }
            }
        }
    }
}
