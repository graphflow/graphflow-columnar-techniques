package ca.waterloo.dsg.graphflow.plan.operator.propertyreader;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.properties.relpropertystore.RelPropertyStore;
import ca.waterloo.dsg.graphflow.storage.graph.properties.relpropertystore.relpropertylists.RelPropertyListBoolean;
import ca.waterloo.dsg.graphflow.storage.graph.properties.relpropertystore.relpropertylists.RelPropertyListDouble;
import ca.waterloo.dsg.graphflow.storage.graph.properties.relpropertystore.relpropertylists.RelPropertyListInteger;
import ca.waterloo.dsg.graphflow.storage.graph.properties.relpropertystore.relpropertylists.RelPropertyListString;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.tuple.value.flat.BoolVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.DoubleVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.RelVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.StringVal;

public abstract class FlatRelPropertyReader extends RelPropertyReader {

    protected RelVal relValToRead;

    public FlatRelPropertyReader(PropertyVariable propertyVariable,
        RelPropertyStore relPropertyStore, Schema inSchema) {
        super(propertyVariable, relPropertyStore,inSchema);
        this.operatorName = "FlatEdgePropertyReader: " + variableToWrite;
    }

    @Override
    public void initFurther(Graph graph) {
        super.initFurther(graph);
        this.outputTuple.set(variableToWriteIdx, ValueFactory.getFlatValueForDataType(
            variableToWrite, valueDataType));
        relValToRead = (RelVal) inputTuple.get(variableToReadIdx);
    }

    public abstract static class IntRelPropertyReader extends FlatRelPropertyReader {

        IntVal valueToWriteTo;
        RelPropertyListInteger propertyList;

        public IntRelPropertyReader(PropertyVariable propertyVariable,
            RelPropertyStore relPropertyStore, Schema inSchema) {
            super(propertyVariable, relPropertyStore,inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (IntVal) outputTuple.get(variableToWriteIdx);
        }

        public static class IntRelPropertyReaderBySrcType extends IntRelPropertyReader {

            public IntRelPropertyReaderBySrcType(PropertyVariable propertyVariable,
                RelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore,inSchema);
                var relVariable = (RelVariable) propertyVariable.getNodeOrRelVariable();
                propertyList = (RelPropertyListInteger) relPropertyStore.getPropertyList(
                    relVariable.getLabel(), relVariable.getSrcNode().getType(),
                    propertyVariable.getPropertyKey());
            }

            @Override
            public void readValues() {
                valueToWriteTo.setInt(propertyList.getProperty(
                    relValToRead.getRelSrcNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset()));
            }
        }

        public static class IntRelPropertyReaderByDstType extends IntRelPropertyReader {

            public IntRelPropertyReaderByDstType(PropertyVariable propertyVariable,
                RelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                var relVariable = (RelVariable) propertyVariable.getNodeOrRelVariable();
                propertyList = (RelPropertyListInteger) relPropertyStore.getPropertyList(
                    relVariable.getLabel(), relVariable.getDstNode().getType(),
                    propertyVariable.getPropertyKey());
            }

            @Override
            public void readValues() {
                valueToWriteTo.setInt(propertyList.getProperty(
                    relValToRead.getRelDstNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset()));
            }
        }
    }

    public abstract static class DoubleRelPropertyReader extends FlatRelPropertyReader {

        DoubleVal valueToWriteTo;
        RelPropertyListDouble propertyList;

        public DoubleRelPropertyReader(PropertyVariable propertyVariable,
            RelPropertyStore relPropertyStore, Schema inSchema) {
            super(propertyVariable, relPropertyStore, inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (DoubleVal) outputTuple.get(variableToWriteIdx);
        }

        public static class DoubleRelPropertyReaderBySrcType extends DoubleRelPropertyReader {

            public DoubleRelPropertyReaderBySrcType(PropertyVariable propertyVariable,
                RelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                var relVariable = (RelVariable) propertyVariable.getNodeOrRelVariable();
                propertyList = (RelPropertyListDouble) relPropertyStore.getPropertyList(
                    relVariable.getLabel(), relVariable.getSrcNode().getType(),
                    propertyVariable.getPropertyKey());
            }

            @Override
            public void readValues() {
                valueToWriteTo.setDouble(propertyList.getProperty(
                    relValToRead.getRelSrcNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset()));
            }
        }

        public static class DoubleRelPropertyReaderByDstType extends DoubleRelPropertyReader {

            public DoubleRelPropertyReaderByDstType(PropertyVariable propertyVariable,
                RelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                var relVariable = (RelVariable) propertyVariable.getNodeOrRelVariable();
                propertyList = (RelPropertyListDouble) relPropertyStore.getPropertyList(
                    relVariable.getLabel(), relVariable.getDstNode().getType(),
                    propertyVariable.getPropertyKey());
            }

            @Override
            public void readValues() {
                valueToWriteTo.setDouble(propertyList.getProperty(
                    relValToRead.getRelDstNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset()));
            }
        }
    }

    public abstract static class StringRelPropertyReader extends FlatRelPropertyReader {

        StringVal valueToWriteTo;
        RelPropertyListString propertyList;

        public StringRelPropertyReader(PropertyVariable propertyVariable,
            RelPropertyStore relPropertyStore, Schema inSchema) {
            super(propertyVariable, relPropertyStore, inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (StringVal) outputTuple.get(variableToWriteIdx);
        }

        public static class StringRelPropertyReaderBySrcType extends StringRelPropertyReader {

            public StringRelPropertyReaderBySrcType(PropertyVariable propertyVariable,
                RelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                var relVariable = (RelVariable) propertyVariable.getNodeOrRelVariable();
                propertyList = (RelPropertyListString) relPropertyStore.getPropertyList(
                    relVariable.getLabel(), relVariable.getSrcNode().getType(),
                    propertyVariable.getPropertyKey());

            }

            @Override
            public void readValues() {
                valueToWriteTo.setString(propertyList.getProperty(
                    relValToRead.getRelSrcNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset()));
            }
        }

        public static class StringRelPropertyReaderByDstType extends StringRelPropertyReader {

            public StringRelPropertyReaderByDstType(PropertyVariable propertyVariable,
                RelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                var relVariable = (RelVariable) propertyVariable.getNodeOrRelVariable();
                propertyList = (RelPropertyListString) relPropertyStore.getPropertyList(
                    relVariable.getLabel(), relVariable.getDstNode().getType(),
                    propertyVariable.getPropertyKey());
            }

            @Override
            public void readValues() {
                valueToWriteTo.setString(propertyList.getProperty(
                    relValToRead.getRelDstNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset()));
            }
        }
    }

    public abstract static class BooleanRelPropertyReader extends FlatRelPropertyReader {

        BoolVal valueToWriteTo;
        RelPropertyListBoolean propertyList;

        public BooleanRelPropertyReader(PropertyVariable propertyVariable,
            RelPropertyStore relPropertyStore, Schema inSchema) {
            super(propertyVariable, relPropertyStore, inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (BoolVal) outputTuple.get(variableToWriteIdx);
        }

        public static class BooleanRelPropertyReaderBySrcType extends BooleanRelPropertyReader {

            public BooleanRelPropertyReaderBySrcType(PropertyVariable propertyVariable,
                RelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                var relVariable = (RelVariable) propertyVariable.getNodeOrRelVariable();
                propertyList = (RelPropertyListBoolean) relPropertyStore.getPropertyList(
                    relVariable.getLabel(), relVariable.getSrcNode().getType(),
                    propertyVariable.getPropertyKey());
            }

            @Override
            public void readValues() {
                valueToWriteTo.setBool(propertyList.getProperty(
                    relValToRead.getRelSrcNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset()));
            }
        }

        public static class BooleanRelPropertyReaderByDstType extends BooleanRelPropertyReader {

            public BooleanRelPropertyReaderByDstType(PropertyVariable propertyVariable,
                RelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                var relVariable = (RelVariable) propertyVariable.getNodeOrRelVariable();
                propertyList = (RelPropertyListBoolean) relPropertyStore.getPropertyList(
                    relVariable.getLabel(), relVariable.getDstNode().getType(),
                    propertyVariable.getPropertyKey());
            }

            @Override
            public void readValues() {
                valueToWriteTo.setBool(propertyList.getProperty(
                    relValToRead.getRelDstNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset()));
            }
        }
    }
}
