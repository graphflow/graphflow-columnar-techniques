package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.util.HashSet;
import java.util.Set;

public abstract class LiteralTerm extends Expression {

    public LiteralTerm(String variableName, DataType dataType) {
        super(variableName, dataType);
    }

    @Override
    public String getPrintableExpression() {
        return getVariableName();
    }

    @Override
    public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
        return () -> {};
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getDependentVariableNames() {
        return new HashSet<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getDependentExpressionVariableNames() {
        return new HashSet<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<PropertyVariable> getDependentPropertyVars() {
        return new HashSet<>();
    }

    public abstract Object getLiteral();
}
