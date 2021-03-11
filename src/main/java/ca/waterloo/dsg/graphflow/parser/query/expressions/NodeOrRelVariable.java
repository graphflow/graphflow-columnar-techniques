package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

public abstract class NodeOrRelVariable extends SimpleVariable {

    protected int typeOrLabel;

    public NodeOrRelVariable(String name) {
        super(name);
    }

    public NodeOrRelVariable(String name, DataType dataType, int typeOrLabel) {
        super(name, dataType);
        this.typeOrLabel = typeOrLabel;
    }

    @Override
    public int hashCode() {
        return super.hashCode()*31 + Objects.hash(typeOrLabel);
    }

    public static class NodeVariable extends NodeOrRelVariable {

        public NodeVariable(String name, int typeOrLabel) {
            super(name, DataType.NODE, typeOrLabel);
        }

        public int getType() {
            return typeOrLabel;
        }
    }

    public static class RelVariable extends NodeOrRelVariable {

        @Setter @Getter NodeVariable srcNode;
        @Setter @Getter NodeVariable dstNode;

        public RelVariable(String varName, int label, NodeVariable srcNode, NodeVariable dstNode) {
            super(varName, DataType.RELATIONSHIP, label);
            this.srcNode = srcNode;
            this.dstNode = dstNode;
        }

        public int getLabel() {
            return typeOrLabel;
        }

        @Override
        public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
            GraphCatalog catalog) {
            if (inputSchema.containsVarName(srcNode.getVariableName())) {
                verifyNodeVariableAndReplace((NodeVariable) inputSchema.getExpression(
                    srcNode.getVariableName()), srcNode, true /*isSrc*/);
            } else if(matchGraphSchema.containsVarName(srcNode.getVariableName())) {
                verifyNodeVariableAndReplace((NodeVariable) matchGraphSchema.getExpression(
                    srcNode.getVariableName()), srcNode, true /*isSrc*/);
            }
            if (GraphCatalog.ANY == srcNode.getType()) {
                throw new MalformedQueryException("Cannot deduce the src node type of " +
                    "relVariable: " + this);
            }
            if (inputSchema.containsVarName(dstNode.getVariableName())) {
                verifyNodeVariableAndReplace((NodeVariable) inputSchema.getExpression(
                    dstNode.getVariableName()), dstNode, false  /*isSrc*/);
            } else if(matchGraphSchema.containsVarName(dstNode.getVariableName())) {
                verifyNodeVariableAndReplace((NodeVariable) matchGraphSchema.getExpression(
                    dstNode.getVariableName()), dstNode, false  /*isSrc*/);
            }
            if (GraphCatalog.ANY == getDstNode().getType()) {
                throw new MalformedQueryException("Cannot deduce the src node type of " +
                    "relVariable: " + this);
            }
        }

        private void verifyNodeVariableAndReplace(NodeVariable prevNodeVariable,
            NodeVariable currentNodeVariable, boolean isSrc) {
            if (prevNodeVariable.getType() != currentNodeVariable.getType() &&
                GraphCatalog.ANY != currentNodeVariable.getType()) {
                throw new MalformedQueryException("Two different vertex types are specified for " +
                    "node: " + currentNodeVariable.getVariableName() + ". " +
                    "First type: " + prevNodeVariable.getType() + "; " +
                    "Second type: " + currentNodeVariable.getType());
            }
            if (isSrc) {
                srcNode = prevNodeVariable;
            } else {
                dstNode = prevNodeVariable;
            }
        }

        @Override
        public int hashCode() {
            var hash = super.hashCode();
            hash = 31*hash + srcNode.hashCode();
            hash = 31*hash + dstNode.hashCode();
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (!super.equals(o)) {
                return false;
            }
            var other = (RelVariable) o;
            return srcNode.equals(other.getSrcNode()) && dstNode.equals(other.getDstNode());
        }
    }
}
