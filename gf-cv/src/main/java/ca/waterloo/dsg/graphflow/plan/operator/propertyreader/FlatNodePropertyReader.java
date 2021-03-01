package ca.waterloo.dsg.graphflow.plan.operator.propertyreader;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore.NodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore.column.ColumnBoolean;
import ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore.column.ColumnDouble;
import ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore.column.ColumnInteger;
import ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore.column.ColumnString;
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
        NodePropertyStore nodePropertyStore, Schema inSchema) {
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
        ColumnInteger column;

        public IntNodePropertyReader(PropertyVariable propertyVariable,
            NodePropertyStore nodePropertyStore, Schema inSchema) {
            super(propertyVariable, nodePropertyStore, inSchema);
            column = (ColumnInteger) nodePropertyStore.getColumn(
                ((NodeVariable) propertyVariable.getNodeOrRelVariable()).getType(),
                propertyVariable.getPropertyKey());
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (IntVal) outputTuple.get(variableToWriteIdx);
        }

        @Override
        public void readValues() {
            valueToWriteTo.setInt(column.getProperty(nodeValToRead.getNodeOffset()));
        }
    }

    public static class DoubleNodePropertyReader extends FlatNodePropertyReader {

        DoubleVal valueToWriteTo;
        ColumnDouble column;

        public DoubleNodePropertyReader(PropertyVariable propertyVariable,
            NodePropertyStore nodePropertyStore, Schema inSchema) {
            super(propertyVariable, nodePropertyStore, inSchema);
            column = (ColumnDouble) nodePropertyStore.getColumn(
                ((NodeVariable) propertyVariable.getNodeOrRelVariable()).getType(),
                propertyVariable.getPropertyKey());
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (DoubleVal) outputTuple.get(variableToWriteIdx);
        }

        @Override
        public void readValues() {
            valueToWriteTo.setDouble(column.getProperty(nodeValToRead.getNodeOffset()));
        }
    }

    public static class StringNodePropertyReader extends FlatNodePropertyReader {

        StringVal valueToWriteTo;
        ColumnString column;

        public StringNodePropertyReader(PropertyVariable propertyVariable,
            NodePropertyStore nodePropertyStore, Schema inSchema) {
            super(propertyVariable, nodePropertyStore, inSchema);
            column = (ColumnString) nodePropertyStore.getColumn(
                ((NodeVariable) propertyVariable.getNodeOrRelVariable()).getType(),
                propertyVariable.getPropertyKey());
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (StringVal) outputTuple.get(variableToWriteIdx);
        }

        @Override
        public void readValues() {
            valueToWriteTo.setString(column.getProperty(nodeValToRead.getNodeOffset()));
        }
    }

    public static class BooleanNodePropertyReader extends FlatNodePropertyReader {

        BoolVal valueToWriteTo;
        ColumnBoolean column;

        public BooleanNodePropertyReader(PropertyVariable propertyVariable,
            NodePropertyStore nodePropertyStore, Schema inSchema) {
            super(propertyVariable, nodePropertyStore, inSchema);
            column = (ColumnBoolean) nodePropertyStore.getColumn(
                ((NodeVariable) propertyVariable.getNodeOrRelVariable()).getType(),
                propertyVariable.getPropertyKey());
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (BoolVal) outputTuple.get(variableToWriteIdx);
        }

        @Override
        public void readValues() {
            valueToWriteTo.setBool(column.getProperty(nodeValToRead.getNodeOffset()));
        }
    }
}
