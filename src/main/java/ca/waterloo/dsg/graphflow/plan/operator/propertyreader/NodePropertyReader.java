package ca.waterloo.dsg.graphflow.plan.operator.propertyreader;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.NodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnBoolean;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnDouble;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnInteger;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnString;

public abstract class NodePropertyReader extends PropertyReader {

    final boolean isFlat;
    final boolean isFiltered;
    protected NodePropertyStore store;

    public NodePropertyReader(PropertyVariable propertyVariable, boolean isFlat,
        boolean isFiltered, NodePropertyStore store, Operator prev) {
        super(propertyVariable, prev);
        this.isFlat = isFlat;
        this.isFiltered = isFiltered;
        this.store = store;
    }

    public static NodePropertyReader make(PropertyVariable variable, boolean isFlat,
        boolean isFiltered, NodePropertyStore store, Operator prev) {
        switch (variable.getDataType()) {
            case INT:
                return new NodePropertyIntReader(variable, isFlat, isFiltered, store, prev);
            case DOUBLE:
                return new NodePropertyDoubleReader(variable, isFlat, isFiltered, store, prev);
            case BOOLEAN:
                return new NodePropertyBoolReader(variable, isFlat, isFiltered, store, prev);
            case STRING:
                return new NodePropertyStringReader(variable, isFlat, isFiltered, store, prev);
            default:
                throw new UnsupportedOperationException("Reading properties for data type: " +
                    variable.getDataType() + " is not yet supported in NodePropertyReader");
        }
    }

    public static class NodePropertyIntReader extends NodePropertyReader {

        ColumnInteger column;

        public NodePropertyIntReader(PropertyVariable variable, boolean isFlat, boolean isFiltered,
            NodePropertyStore store, Operator prev) {
            super(variable, isFlat, isFiltered, store, prev);
            this.column = (ColumnInteger) store.getColumn(((NodeVariable) variable.
                getNodeOrRelVariable()).getType(), variable.getPropertyKey());
        }

        @Override
        protected void readValues() {
            if (isFlat) {
                var pos = inVector.state.getCurrSelectedValuesPos();
                outVector.set(pos, column.getProperty(inVector.getNodeOffset(pos)));
            } else if (isFiltered) {
                for (var i = 0; i < inVector.state.size; i++) {
                    var pos = inVector.state.selectedValuesPos[i];
                    outVector.set(pos, column.getProperty(inVector.getNodeOffset(pos)));
                }
            } else {
                it.init();
                for (var i = 0; i < inVector.state.size; i++) {
                    outVector.set(i, column.getProperty(it.getNextNodeOffset()));
                    it.moveCursor();
                }
            }
        }

        @Override
        public NodePropertyIntReader copy() {
            return new NodePropertyIntReader(variable, isFlat, isFiltered, store, prev.copy());
        }
    }

    public static class NodePropertyDoubleReader extends NodePropertyReader {

        ColumnDouble column;

        public NodePropertyDoubleReader(PropertyVariable variable, boolean isFlat,
            boolean isFiltered, NodePropertyStore store, Operator prev) {
            super(variable, isFlat, isFiltered, store, prev);
            this.column = (ColumnDouble) store.getColumn(((NodeVariable) variable.
                getNodeOrRelVariable()).getType(), variable.getPropertyKey());
        }

        @Override
        protected void readValues() {
            if (isFlat) {
                var pos = inVector.state.getCurrSelectedValuesPos();
                outVector.set(pos, column.getProperty(inVector.getNodeOffset(pos)));
            } else if (isFiltered) {
                for (var i = 0; i < inVector.state.size; i++) {
                    var pos = inVector.state.selectedValuesPos[i];
                    outVector.set(pos, column.getProperty(inVector.getNodeOffset(pos)));
                }
            } else {
                it.init();
                for (var i = 0; i < inVector.state.size; i++) {
                    outVector.set(i, column.getProperty(it.getNextNodeOffset()));
                    it.moveCursor();
                }
            }
        }

        @Override
        public NodePropertyDoubleReader copy() {
            return new NodePropertyDoubleReader(variable, isFlat, isFiltered, store, prev.copy());
        }
    }

    public static class NodePropertyBoolReader extends NodePropertyReader {

        ColumnBoolean column;

        public NodePropertyBoolReader(PropertyVariable variable, boolean isFlat, boolean isFiltered,
            NodePropertyStore store, Operator prev) {
            super(variable, isFlat, isFiltered, store, prev);
            this.column = (ColumnBoolean) store.getColumn(((NodeVariable) variable.
                getNodeOrRelVariable()).getType(), variable.getPropertyKey());
        }

        @Override
        protected void readValues() {
            if (isFlat) {
                var pos = inVector.state.getCurrSelectedValuesPos();
                outVector.set(pos, column.getProperty(inVector.getNodeOffset(pos)));
            } else if (isFiltered) {
                for (var i = 0; i < inVector.state.size; i++) {
                    var pos = inVector.state.selectedValuesPos[i];
                    outVector.set(pos, column.getProperty(inVector.getNodeOffset(pos)));
                }
            } else {
                it.init();
                for (var i = 0; i < inVector.state.size; i++) {
                    outVector.set(i, column.getProperty(it.getNextNodeOffset()));
                    it.moveCursor();
                }
            }
        }

        @Override
        public NodePropertyBoolReader copy() {
            return new NodePropertyBoolReader(variable, isFlat, isFiltered, store, prev.copy());
        }
    }

    public static class NodePropertyStringReader extends NodePropertyReader {

        ColumnString column;

        public NodePropertyStringReader(PropertyVariable variable, boolean isFlat,
            boolean isFiltered, NodePropertyStore store, Operator prev) {
            super(variable, isFlat, isFiltered, store, prev);
            this.column = (ColumnString) store.getColumn(((NodeVariable) variable.
                getNodeOrRelVariable()).getType(), variable.getPropertyKey());
        }

        @Override
        protected void readValues() {
            if (isFlat) {
                var pos = inVector.state.getCurrSelectedValuesPos();
                outVector.set(pos, column.getProperty(inVector.getNodeOffset(pos)));
            } else if (isFiltered) {
                for (var i = 0; i < inVector.state.size; i++) {
                    var pos = inVector.state.selectedValuesPos[i];
                    outVector.set(pos, column.getProperty(inVector.getNodeOffset(pos)));
                }
            } else {
                it.init();
                for (var i = 0; i < inVector.state.size; i++) {
                    var nodeOffset = it.getNextNodeOffset();
                    if (column == null) {
                        System.out.println("column == null.");
                    }
                    var property = column.getProperty(nodeOffset);
                    outVector.set(i, property);
                    it.moveCursor();
                }
            }
        }

        @Override
        public NodePropertyStringReader copy() {
            return new NodePropertyStringReader(variable, isFlat, isFiltered, store, prev.copy());
        }
    }
}
