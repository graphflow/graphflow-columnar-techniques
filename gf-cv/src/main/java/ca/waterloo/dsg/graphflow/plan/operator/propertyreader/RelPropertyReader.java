package ca.waterloo.dsg.graphflow.plan.operator.propertyreader;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatRelPropertyReader.BooleanRelPropertyReader.BooleanRelPropertyReaderByDstType;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatRelPropertyReader.BooleanRelPropertyReader.BooleanRelPropertyReaderBySrcType;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatRelPropertyReader.DoubleRelPropertyReader.DoubleRelPropertyReaderByDstType;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatRelPropertyReader.DoubleRelPropertyReader.DoubleRelPropertyReaderBySrcType;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatRelPropertyReader.IntRelPropertyReader.IntRelPropertyReaderByDstType;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatRelPropertyReader.IntRelPropertyReader.IntRelPropertyReaderBySrcType;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatRelPropertyReader.StringRelPropertyReader.StringRelPropertyReaderByDstType;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.FlatRelPropertyReader.StringRelPropertyReader.StringRelPropertyReaderBySrcType;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.graph.properties.relpropertystore.RelPropertyStore;

import ca.waterloo.dsg.graphflow.tuple.Schema;

public abstract class RelPropertyReader extends PropertyReader  {

    RelPropertyStore relPropertyStore;
    int key;
    int label;
    int type;

    public RelPropertyReader(PropertyVariable propertyVariable,
        RelPropertyStore relPropertyStore, Schema inSchema) {
        super(propertyVariable, inSchema);
        this.relPropertyStore = relPropertyStore;
        label = ((RelVariable) propertyVariable.getNodeOrRelVariable()).getLabel();
        key = propertyVariable.getPropertyKey();
    }

    public static RelPropertyReader make(PropertyVariable propertyVariable,
        RelPropertyStore relPropertyStore, Schema inSchema, GraphCatalog catalog) {
        var label = ((RelVariable) propertyVariable.getNodeOrRelVariable()).getLabel();
        if (!catalog.labelDirectionHasMultiplicityOne(label, Direction.FORWARD) &&
            catalog.labelDirectionHasMultiplicityOne(label, Direction.BACKWARD)) {
            switch (propertyVariable.getDataType()) {
                case INT:
                    return new IntRelPropertyReaderByDstType(propertyVariable, relPropertyStore,
                            inSchema);
                case DOUBLE:
                    return new DoubleRelPropertyReaderByDstType(propertyVariable, relPropertyStore,
                            inSchema);
                case STRING:
                    return new StringRelPropertyReaderByDstType(propertyVariable, relPropertyStore,
                            inSchema);
                case BOOLEAN:
                    return new BooleanRelPropertyReaderByDstType(propertyVariable, relPropertyStore,
                            inSchema);
                default:
                    throw new UnsupportedOperationException("Reading vertex properties for " +
                        "data type: " + propertyVariable.getDataType() + " is not yet " +
                        "supported in " + RelPropertyReader.class.getSimpleName());
            }
        } else {
            switch (propertyVariable.getDataType()) {
                case INT:
                    return new IntRelPropertyReaderBySrcType(propertyVariable, relPropertyStore,
                        inSchema);
                case DOUBLE:
                    return new DoubleRelPropertyReaderBySrcType(propertyVariable, relPropertyStore,
                            inSchema);
                case STRING:
                    return new StringRelPropertyReaderBySrcType(propertyVariable, relPropertyStore,
                            inSchema);
                case BOOLEAN:
                    return new BooleanRelPropertyReaderBySrcType(propertyVariable, relPropertyStore,
                            inSchema);
                default:
                    throw new UnsupportedOperationException("Reading vertex properties for " +
                        "data type: " + propertyVariable.getDataType() + " is not yet " +
                        "supported in " + RelPropertyReader.class.getSimpleName());
            }
        }
    }
}
