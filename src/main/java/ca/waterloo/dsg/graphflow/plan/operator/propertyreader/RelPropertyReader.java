package ca.waterloo.dsg.graphflow.plan.operator.propertyreader;

import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.relpropertylists.RelPropertyListDouble;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.relpropertylists.RelPropertyListInteger;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.relpropertylists.RelPropertyListString;

public abstract class RelPropertyReader extends PropertyReader {

    final boolean isFlat;
    final boolean isFiltered;
    boolean isSrcNodeInAdjEdgesVector;

    final boolean byDstType;
    protected Vector srcVectorNode;
    protected PropertyVariable propertyVariable;

    public RelPropertyReader(PropertyVariable variable, boolean byDstType, boolean isFlat,
        boolean isFiltered, Operator prev) {
        super(variable, prev);
        this.byDstType = byDstType;
        this.isFlat = isFlat;
        this.isFiltered = isFiltered;
        this.propertyVariable = variable;
    }

    public static RelPropertyReader make(PropertyVariable variable, boolean isFlat, boolean isFiltered,
        GraphCatalog catalog, Operator prev) {
        var label = ((RelVariable) variable.getNodeOrRelVariable()).getLabel();
        var isDstType = !catalog.labelDirectionHasMultiplicityOne(label, Direction.FORWARD) &&
            catalog.labelDirectionHasMultiplicityOne(label, Direction.BACKWARD);
        switch (variable.getDataType()) {
            case INT:
                return new RelPropertyIntReader(variable, isDstType, isFlat, isFiltered, prev);
            case DOUBLE:
                return new RelPropertyDoubleReader(variable, isDstType, isFlat, isFiltered, prev);
            case STRING:
                return new RelPropertyStringReader(variable, isDstType, isFlat, isFiltered, prev);
            default:
                throw new UnsupportedOperationException("Reading properties for data type: " +
                    variable.getDataType() + " is not yet supported in RelPropertyReader");
        }
    }

    public static class RelPropertyIntReader extends RelPropertyReader {

        RelPropertyListInteger list;

        public RelPropertyIntReader(PropertyVariable propertyVariable, boolean byDstType,
            boolean isFlat, boolean isFiltered, Operator prev) {
            super(propertyVariable, byDstType, isFlat, isFiltered, prev);
        }

        @Override
        protected void initFurther(Graph graph) {
            super.initFurther(graph);
            var propertyKey = graph.getGraphCatalog().getRelPropertyKey(variable.getPropertyName());
            var relVariable = (RelVariable) variable.getNodeOrRelVariable();
            var srcNodeVariable = byDstType ? relVariable.getDstNode() : relVariable.getSrcNode();
            var srcVarName = srcNodeVariable.getVariableName();
            srcVectorNode = dataChunks.getValueVector(srcVarName);
            isSrcNodeInAdjEdgesVector = dataChunks.isFlat(srcVarName);
            list = (RelPropertyListInteger) graph.getRelPropertyStore().getPropertyList(
                relVariable.getLabel(), srcNodeVariable.getType(), propertyKey);
        }

        @Override
        protected void readValues() {
            /* inVector is the AdjEdgesVector storing (nodeID, bucketOffset) pairs */
            if (isFlat) {
                if (isSrcNodeInAdjEdgesVector) {
                    var pos = srcVectorNode.state.getCurrSelectedValuesPos();
                    var nodeOffset = srcVectorNode.getNodeOffset(pos);
                    pos = inVector.state.getCurrSelectedValuesPos();
                    outVector.set(pos, list.getProperty(nodeOffset, inVector.getRelBucketOffset(pos)));
                } else {
                    var pos = inVector.state.getCurrSelectedValuesPos();
                    outVector.set(pos, list.getProperty(inVector.getNodeOffset(pos),
                        inVector.getRelBucketOffset(pos)));
                }
            } else {
                if (isSrcNodeInAdjEdgesVector) {
                    var pos = srcVectorNode.state.getCurrSelectedValuesPos();
                    var nodeOffset = srcVectorNode.getNodeOffset(pos);
                    for (var i = 0; i < inVector.state.size; i++) {
                        pos = inVector.state.selectedValuesPos[i];
                        outVector.set(pos, list.getProperty(nodeOffset,
                            inVector.getRelBucketOffset(pos)));
                    }
                } else if (isFiltered) {
                    for (var i = 0; i < inVector.state.size; i++) {
                        var pos = inVector.state.selectedValuesPos[i];
                        outVector.set(pos, list.getProperty(inVector.getNodeOffset(pos),
                            inVector.getRelBucketOffset(pos)));
                    }
                } else {
                    it.init();
                    for (var i = 0; i < inVector.state.size; i++) {
                        outVector.set(i, list.getProperty(srcVectorNode.getNodeOffset(i),
                            it.getNextRelBucketOffset()));
                        it.moveCursor();
                    }
                }
            }
        }

        @Override
        public RelPropertyIntReader copy() {
            return new RelPropertyIntReader(propertyVariable, byDstType, isFlat, isFiltered,
                prev.copy());
        }
    }

    public static class RelPropertyDoubleReader extends RelPropertyReader {

        RelPropertyListDouble list;

        public RelPropertyDoubleReader(PropertyVariable propertyVariable, boolean byDstType,
            boolean isFlat, boolean isFiltered, Operator prev) {
            super(propertyVariable, byDstType, isFlat, isFiltered, prev);
        }

        @Override
        protected void initFurther(Graph graph) {
            super.initFurther(graph);
            var propertyKey = graph.getGraphCatalog().getRelPropertyKey(variable.getPropertyName());
            var relVariable = (RelVariable) variable.getNodeOrRelVariable();
            var srcNodeVariable = byDstType ? relVariable.getDstNode() : relVariable.getSrcNode();
            var srcVarName = srcNodeVariable.getVariableName();
            srcVectorNode = dataChunks.getValueVector(srcVarName);
            isSrcNodeInAdjEdgesVector = dataChunks.isFlat(srcVarName);
            list = (RelPropertyListDouble) graph.getRelPropertyStore().getPropertyList(
                relVariable.getLabel(), srcNodeVariable.getType(), propertyKey);
        }

        @Override
        protected void readValues() {
            /* inVector is the AdjEdgesVector storing (nodeID, bucketOffset) pairs */
            if (isFlat) {
                if (isSrcNodeInAdjEdgesVector) {
                    var pos = srcVectorNode.state.getCurrSelectedValuesPos();
                    var nodeOffset = srcVectorNode.getNodeOffset(pos);
                    pos = inVector.state.getCurrSelectedValuesPos();
                    outVector.set(pos, list.getProperty(nodeOffset, inVector.getRelBucketOffset(pos)));
                } else {
                    var pos = inVector.state.getCurrSelectedValuesPos();
                    outVector.set(pos, list.getProperty(inVector.getNodeOffset(pos),
                        inVector.getRelBucketOffset(pos)));
                }
            } else {
                if (isSrcNodeInAdjEdgesVector) {
                    var pos = srcVectorNode.state.getCurrSelectedValuesPos();
                    var nodeOffset = srcVectorNode.getNodeOffset(pos);
                    for (var i = 0; i < inVector.state.size; i++) {
                        pos = inVector.state.selectedValuesPos[i];
                        outVector.set(pos, list.getProperty(nodeOffset,
                            inVector.getRelBucketOffset(pos)));
                    }
                } else if (isFiltered) {
                    for (var i = 0; i < inVector.state.size; i++) {
                        var pos = inVector.state.selectedValuesPos[i];
                        outVector.set(pos, list.getProperty(inVector.getNodeOffset(pos),
                            inVector.getRelBucketOffset(pos)));
                    }
                } else {
                    it.init();
                    for (var i = 0; i < inVector.state.size; i++) {
                        outVector.set(i, list.getProperty(srcVectorNode.getNodeOffset(i),
                            it.getNextRelBucketOffset()));
                        it.moveCursor();
                    }
                }
            }
        }

        @Override
        public RelPropertyDoubleReader copy() {
            return new RelPropertyDoubleReader(propertyVariable, byDstType, isFlat, isFiltered,
                prev.copy());
        }
    }

    public static class RelPropertyStringReader extends RelPropertyReader {

        RelPropertyListString list;

        public RelPropertyStringReader(PropertyVariable propertyVariable, boolean byDstType,
            boolean isFlat, boolean isFiltered, Operator prev) {
            super(propertyVariable, byDstType, isFlat, isFiltered, prev);
        }

        @Override
        protected void initFurther(Graph graph) {
            super.initFurther(graph);
            var propertyKey = graph.getGraphCatalog().getRelPropertyKey(variable.getPropertyName());
            var relVariable = (RelVariable) variable.getNodeOrRelVariable();
            var srcNodeVariable = byDstType ? relVariable.getDstNode() : relVariable.getSrcNode();
            var srcVarName = srcNodeVariable.getVariableName();
            srcVectorNode = dataChunks.getValueVector(srcVarName);
            isSrcNodeInAdjEdgesVector = dataChunks.isFlat(srcVarName);
            list = (RelPropertyListString) graph.getRelPropertyStore().getPropertyList(
                relVariable.getLabel(), srcNodeVariable.getType(), propertyKey);
        }

        @Override
        protected void readValues() {
            /* inVector is the AdjEdgesVector storing (nodeID, bucketOffset) pairs */
            if (isFlat) {
                if (isSrcNodeInAdjEdgesVector) {
                    var pos = srcVectorNode.state.getCurrSelectedValuesPos();
                    var nodeOffset = srcVectorNode.getNodeOffset(pos);
                    pos = inVector.state.getCurrSelectedValuesPos();
                    outVector.set(pos, list.getProperty(nodeOffset, inVector.getRelBucketOffset(pos)));
                } else {
                    var pos = inVector.state.getCurrSelectedValuesPos();
                    outVector.set(pos, list.getProperty(inVector.getNodeOffset(pos),
                        inVector.getRelBucketOffset(pos)));
                }
            } else {
                if (isSrcNodeInAdjEdgesVector) {
                    var pos = srcVectorNode.state.getCurrSelectedValuesPos();
                    var nodeOffset = srcVectorNode.getNodeOffset(pos);
                    for (var i = 0; i < inVector.state.size; i++) {
                        pos = inVector.state.selectedValuesPos[i];
                        outVector.set(pos, list.getProperty(nodeOffset,
                            inVector.getRelBucketOffset(pos)));
                    }
                } else if (isFiltered) {
                    for (var i = 0; i < inVector.state.size; i++) {
                        var pos = inVector.state.selectedValuesPos[i];
                        outVector.set(pos, list.getProperty(inVector.getNodeOffset(pos),
                            inVector.getRelBucketOffset(pos)));
                    }
                } else {
                    it.init();
                    for (var i = 0; i < inVector.state.size; i++) {
                        outVector.set(i, list.getProperty(srcVectorNode.getNodeOffset(i),
                            it.getNextRelBucketOffset()));
                        it.moveCursor();
                    }
                }
            }
        }

        @Override
        public RelPropertyStringReader copy() {
            return new RelPropertyStringReader(propertyVariable, byDstType, isFlat, isFiltered,
                prev.copy());
        }
    }
}
