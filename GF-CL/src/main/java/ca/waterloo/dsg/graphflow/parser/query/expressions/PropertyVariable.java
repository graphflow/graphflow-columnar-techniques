package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class PropertyVariable extends Expression {

    @Getter private SimpleVariable nodeOrRelVariable;
    @Getter private String propertyName;
    @Getter @Setter private int propertyKey;

    public PropertyVariable(SimpleVariable nodeOrRelVariable, String propertyName) {
        super(nodeOrRelVariable.getVariableName() + "." + propertyName);
        this.nodeOrRelVariable = nodeOrRelVariable;
        this.propertyName = propertyName;
    }

    public PropertyVariable(SimpleVariable nodeOrRelVariable, String propertyName, DataType dataType) {
        this(nodeOrRelVariable, propertyName);
        this.dataType = dataType;
    }

    @Override
    public Set<String> getDependentVariableNames() {
        return nodeOrRelVariable.getDependentVariableNames();
    }

    @Override
    public Set<String> getDependentExpressionVariableNames() {
        HashSet<String> retVal = new HashSet<>();
        retVal.add(variableName);
        return retVal;
    }

    @Override
    public Set<PropertyVariable> getDependentPropertyVars() {
        Set<PropertyVariable> retVal = new HashSet<>();
        retVal.add(this);
        return retVal;
    }

    @Override
    public void verifyVariablesAndNormalize(Schema inputSchema,
        Schema matchGraphSchema, GraphCatalog catalog) {
        if (inputSchema.containsVarName(nodeOrRelVariable.getVariableName())) {
            nodeOrRelVariable = (NodeOrRelVariable) inputSchema.getExpression(
                nodeOrRelVariable.getVariableName());
        } else if (matchGraphSchema.containsVarName(nodeOrRelVariable.getVariableName())) {
            nodeOrRelVariable = (NodeOrRelVariable) matchGraphSchema.getExpression(
                nodeOrRelVariable.getVariableName());
        } else {
            throw new MalformedQueryException("Variable: " + variableName + " is not in scope");
        }
        if (DataType.NODE == nodeOrRelVariable.getDataType()) {
            propertyKey = catalog.getNodePropertyKey(propertyName);
            setDataType(catalog.getNodePropertyDataType(propertyKey));
        } else {
            propertyKey = catalog.getRelPropertyKey(propertyName);
            setDataType(catalog.getRelPropertyDataType(propertyKey));
        }
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        hash = 31*hash + nodeOrRelVariable.hashCode();
        hash = 31*hash + Objects.hash(propertyName, propertyKey);
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var other = (PropertyVariable) o;
        return this.nodeOrRelVariable.equals(other.nodeOrRelVariable)
            && this.propertyName.equals(other.propertyName)
            && this.propertyKey == other.propertyKey;
    }
}
