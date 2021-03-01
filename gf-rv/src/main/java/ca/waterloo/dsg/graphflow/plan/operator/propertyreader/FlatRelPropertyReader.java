package ca.waterloo.dsg.graphflow.plan.operator.propertyreader;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.properties.relpropertystore.UnstructuredRelPropertyStore;
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
        UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
        super(propertyVariable, relPropertyStore, inSchema);
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

        public IntRelPropertyReader(PropertyVariable propertyVariable,
            UnstructuredRelPropertyStore relPropertyStore,Schema inSchema) {
            super(propertyVariable, relPropertyStore, inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (IntVal) outputTuple.get(variableToWriteIdx);
        }

        public static class IntRelPropertyReaderBySrcType extends IntRelPropertyReader {

            public IntRelPropertyReaderBySrcType(PropertyVariable propertyVariable,
                UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                type = ((RelVariable) propertyVariable.getNodeOrRelVariable()).getSrcNode().getType();
            }

            @Override
            public void readValues() {
                valueToWriteTo.setInt(relPropertyStore.getInt(label, type,
                    relValToRead.getRelSrcNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset(), key));
            }
        }

        public static class IntRelPropertyReaderByDstType extends IntRelPropertyReader {

            public IntRelPropertyReaderByDstType(PropertyVariable propertyVariable,
                UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                type = ((RelVariable) propertyVariable.getNodeOrRelVariable()).getDstNode().getType();
            }

            @Override
            public void readValues() {
                valueToWriteTo.setInt(relPropertyStore.getInt(label, type,
                    relValToRead.getRelDstNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset(), key));
            }
        }
    }

    public abstract static class DoubleRelPropertyReader extends FlatRelPropertyReader {

        DoubleVal valueToWriteTo;

        public DoubleRelPropertyReader(PropertyVariable propertyVariable,
            UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
            super(propertyVariable, relPropertyStore, inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (DoubleVal) outputTuple.get(variableToWriteIdx);
        }

        public static class DoubleRelPropertyReaderBySrcType extends DoubleRelPropertyReader {

            public DoubleRelPropertyReaderBySrcType(PropertyVariable propertyVariable,
                UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                type = ((RelVariable) propertyVariable.getNodeOrRelVariable()).getSrcNode().getType();
            }

            @Override
            public void readValues() {
                valueToWriteTo.setDouble(relPropertyStore.getDouble(label, type,
                    relValToRead.getRelSrcNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset(), key));
            }
        }

        public static class DoubleRelPropertyReaderByDstType extends DoubleRelPropertyReader {

            public DoubleRelPropertyReaderByDstType(PropertyVariable propertyVariable,
                UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                type = ((RelVariable) propertyVariable.getNodeOrRelVariable()).getDstNode().getType();
            }

            @Override
            public void readValues() {
                valueToWriteTo.setDouble(relPropertyStore.getDouble(label, type,
                    relValToRead.getRelDstNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset(), key));
            }
        }
    }

    public abstract static class StringRelPropertyReader extends FlatRelPropertyReader {

        StringVal valueToWriteTo;

        public StringRelPropertyReader(PropertyVariable propertyVariable,
            UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
            super(propertyVariable, relPropertyStore, inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (StringVal) outputTuple.get(variableToWriteIdx);
        }

        public static class StringRelPropertyReaderBySrcType extends StringRelPropertyReader {

            public StringRelPropertyReaderBySrcType(PropertyVariable propertyVariable,
                UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                type = ((RelVariable) propertyVariable.getNodeOrRelVariable()).getSrcNode().getType();
            }

            @Override
            public void readValues() {
                valueToWriteTo.setString(relPropertyStore.getString(label, type,
                    relValToRead.getRelSrcNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset(), key));
            }
        }

        public static class StringRelPropertyReaderByDstType extends StringRelPropertyReader {

            public StringRelPropertyReaderByDstType(PropertyVariable propertyVariable,
                UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                type = ((RelVariable) propertyVariable.getNodeOrRelVariable()).getDstNode().getType();
            }

            @Override
            public void readValues() {
                valueToWriteTo.setString(relPropertyStore.getString(label, type,
                    relValToRead.getRelDstNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset(), key));
            }
        }
    }

    public abstract static class BooleanRelPropertyReader extends FlatRelPropertyReader {

        BoolVal valueToWriteTo;

        public BooleanRelPropertyReader(PropertyVariable propertyVariable,
            UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
            super(propertyVariable, relPropertyStore, inSchema);
        }

        @Override
        public void initFurther(Graph graph) {
            super.initFurther(graph);
            valueToWriteTo = (BoolVal) outputTuple.get(variableToWriteIdx);
        }

        public static class BooleanRelPropertyReaderBySrcType extends BooleanRelPropertyReader {

            public BooleanRelPropertyReaderBySrcType(PropertyVariable propertyVariable,
                UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                type = ((RelVariable) propertyVariable.getNodeOrRelVariable()).getSrcNode().getType();
            }

            @Override
            public void readValues() {
                valueToWriteTo.setBool(relPropertyStore.getBoolean(label, type,
                    relValToRead.getRelSrcNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset(), key));
            }
        }

        public static class BooleanRelPropertyReaderByDstType extends BooleanRelPropertyReader {

            public BooleanRelPropertyReaderByDstType(PropertyVariable propertyVariable,
                UnstructuredRelPropertyStore relPropertyStore, Schema inSchema) {
                super(propertyVariable, relPropertyStore, inSchema);
                type = ((RelVariable) propertyVariable.getNodeOrRelVariable()).getDstNode().getType();
            }

            @Override
            public void readValues() {
                valueToWriteTo.setBool(relPropertyStore.getBoolean(label, type,
                    relValToRead.getRelDstNodeVal().getNodeOffset(),
                    relValToRead.getRelBucketOffset(), key));
            }
        }
    }
}
