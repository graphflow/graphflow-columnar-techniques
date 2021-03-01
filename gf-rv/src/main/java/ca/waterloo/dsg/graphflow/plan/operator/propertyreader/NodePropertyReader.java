package ca.waterloo.dsg.graphflow.plan.operator.propertyreader;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatNodePropertyReader.BooleanNodePropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatNodePropertyReader.DoubleNodePropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatNodePropertyReader.IntNodePropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatNodePropertyReader.StringNodePropertyReader;
import ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore.UnstructuredNodePropertyStore;
import ca.waterloo.dsg.graphflow.tuple.Schema;

public abstract class NodePropertyReader extends PropertyReader {

    UnstructuredNodePropertyStore nodePropertyStore;
    int propertyKey;
    int type;

    public NodePropertyReader(PropertyVariable propertyVariable,
        UnstructuredNodePropertyStore nodePropertyStore, Schema inSchema) {
        super(propertyVariable, inSchema);
        this.nodePropertyStore = nodePropertyStore;
        type = ((NodeVariable) propertyVariable.getNodeOrRelVariable()).getType();
        propertyKey = propertyVariable.getPropertyKey();
    }

    public static NodePropertyReader make(PropertyVariable propertyVariable,
        UnstructuredNodePropertyStore nodePropertyStore, Schema inSchema) {
        switch (propertyVariable.getDataType()) {
            case INT:
                return new IntNodePropertyReader(propertyVariable, nodePropertyStore, inSchema);
            case DOUBLE:
                return new DoubleNodePropertyReader(propertyVariable, nodePropertyStore, inSchema);
            case STRING:
                return new StringNodePropertyReader(propertyVariable, nodePropertyStore, inSchema);
            case BOOLEAN:
                return new BooleanNodePropertyReader(propertyVariable, nodePropertyStore, inSchema);
            default:
                throw new UnsupportedOperationException("Reading vertex properties for " +
                    "data type: " + propertyVariable.getDataType() + " is not yet " +
                    "supported in " + NodePropertyReader.class.getSimpleName());
        }
    }
}
