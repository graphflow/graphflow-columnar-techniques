package ca.waterloo.dsg.graphflow.plan.operator.propertyreader;

import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore.UnstructuredNodePropertyStore;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.tuple.value.flat.BoolVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.DoubleVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.StringVal;

public abstract class FlatNodePropertyReader extends NodePropertyReader {

    protected NodeVal nodeValToRead;

    public FlatNodePropertyReader(PropertyVariable propertyVariable,
        UnstructuredNodePropertyStore nodePropertyStore, Schema inSchema) {
        super(propertyVariable, nodePropertyStore, inSchema);
        this.operatorName = "FlatVertexPropertyReader: " + variableToWrite;
    }

    @Override
    public void initFurther(Graph graph) {
        super.initFurther(graph);
        this.outputTuple.set(variableToWriteIdx, ValueFactory.getFlatValueForDataType(
            variableToWrite, valueDataType));
        nodeValToRead = (NodeVal) inputTuple.get(variableToReadIdx);
    }

    public static class IntNodePropertyReader extends FlatNodePropertyReader {

        IntVal valueToWriteTo;

        public IntNodePropertyReader(PropertyVariable propertyVariable,
            UnstructuredNodePropertyStore nodePropertyStore, Schema inSchema) {
            super(propertyVariable, nodePropertyStore, inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (IntVal) outputTuple.get(variableToWriteIdx);
        }

        @Override
        public void readValues() {
            valueToWriteTo.setInt(nodePropertyStore.getInt(type, nodeValToRead.getNodeOffset(),
                propertyKey));
        }
    }

    public static class DoubleNodePropertyReader extends FlatNodePropertyReader {

        DoubleVal valueToWriteTo;

        public DoubleNodePropertyReader(PropertyVariable propertyVariable,
            UnstructuredNodePropertyStore nodePropertyStore, Schema inSchema) {
            super(propertyVariable, nodePropertyStore, inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (DoubleVal) outputTuple.get(variableToWriteIdx);
        }

        @Override
        public void readValues() {
            valueToWriteTo.setDouble(nodePropertyStore.getDouble(type, nodeValToRead.getNodeOffset(),
                propertyKey));
        }
    }

    public static class StringNodePropertyReader extends FlatNodePropertyReader {

        StringVal valueToWriteTo;

        public StringNodePropertyReader(PropertyVariable propertyVariable,
            UnstructuredNodePropertyStore nodePropertyStore, Schema inSchema) {
            super(propertyVariable, nodePropertyStore, inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (StringVal) outputTuple.get(variableToWriteIdx);
        }

        @Override
        public void readValues() {
            valueToWriteTo.setString(nodePropertyStore.getString(type, nodeValToRead.getNodeOffset(), propertyKey));
        }
    }

    public static class BooleanNodePropertyReader extends FlatNodePropertyReader {

        BoolVal valueToWriteTo;

        public BooleanNodePropertyReader(PropertyVariable propertyVariable,
            UnstructuredNodePropertyStore nodePropertyStore, Schema inSchema) {
            super(propertyVariable, nodePropertyStore, inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (BoolVal) outputTuple.get(variableToWriteIdx);
        }

        @Override
        public void readValues() {
            valueToWriteTo.setBool(nodePropertyStore.getBoolean(type, nodeValToRead.getNodeOffset(),
                propertyKey));
        }
    }
}
