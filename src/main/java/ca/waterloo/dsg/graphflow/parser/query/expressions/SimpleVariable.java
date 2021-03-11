package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.util.HashSet;
import java.util.Set;

public class SimpleVariable extends Expression {

    public SimpleVariable(String name) {
        super(name);
    }

    public SimpleVariable(String name, DataType dataType) {
        super(name, dataType);
    }

    @Override
    public Set<String> getDependentVariableNames() {
        HashSet<String> retVal = new HashSet<>();
        retVal.add(variableName);
        return retVal;
    }

    @Override
    public Set<String> getDependentExpressionVariableNames() {
        HashSet<String> retVal = new HashSet<>();
        retVal.add(variableName);
        return retVal;
    }

    @Override
    public Set<PropertyVariable> getDependentPropertyVars() {
        return new HashSet<>();
    }

    @Override
    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {
        if (!inputSchema.containsVarName(variableName) &&
            !matchGraphSchema.containsVarName(variableName)) {
            throw new MalformedQueryException("Variable: " + variableName + " is not in scope");
        }
        if (DataType.UNKNOWN == dataType) {
            if (inputSchema.containsVarName(variableName)) {
                setDataType(inputSchema.getExpression(variableName).getDataType());
            } else {
                setDataType(matchGraphSchema.getExpression(variableName).getDataType());
            }
        }
    }
}
