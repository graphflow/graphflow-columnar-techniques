package ca.waterloo.dsg.graphflow.util;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

public abstract class GraphflowTestUtils {

    public static NodeVariable getNodeVariable(String varName) {
        return new NodeVariable(varName, 0 /*nodeType*/);
    }

    public static RelVariable getRelVariable(String varName, NodeVariable srcNode,
        NodeVariable dstNode) {
        return new RelVariable(varName, 1 /*edgeLabel*/, srcNode, dstNode);
    }

    public static PropertyVariable getPropertyVariable(String propertyName, int propertyKey,
        DataType propertyDataType, NodeOrRelVariable nodeOrRelVariable) {
        var propertyVariable = new PropertyVariable(nodeOrRelVariable, propertyName);
        propertyVariable.setPropertyKey(propertyKey);
        propertyVariable.setDataType(propertyDataType);
        return propertyVariable;
    }

    public static GraphCatalog getMockedGraphCatalog() {
        var catalog = Mockito.mock(GraphCatalog.class);
        Mockito.when(catalog.getTypeKey(anyString())).thenReturn(0);
        Mockito.when(catalog.getLabelKey(anyString())).thenReturn(1);
        Mockito.when(catalog.getNodePropertyKey(anyString())).thenReturn(2);
        Mockito.when(catalog.getRelPropertyKey(anyString())).thenReturn(2);
        Mockito.when(catalog.getNodePropertyDataType(anyInt())).thenReturn(DataType.INT);
        Mockito.when(catalog.getRelPropertyDataType(anyInt())).thenReturn(DataType.INT);
        Mockito.when(catalog.typeLabelExistsForDirection(0, 1, Direction.FORWARD)).thenReturn(true);
        Mockito.when(catalog.typeLabelExistsForDirection(0, 1, Direction.BACKWARD)).thenReturn(
            true);
        return catalog;
    }

    public static GraphCatalog getMockedGraphCatalogReturningStringProperty() {
        var catalog = getMockedGraphCatalog();
        Mockito.when(catalog.getNodePropertyDataType((short) 2)).thenReturn(DataType.STRING);
        Mockito.when(catalog.getRelPropertyDataType((short) 2)).thenReturn(DataType.STRING);
        return catalog;
    }
}
