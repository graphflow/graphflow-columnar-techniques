package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

public abstract class LiteralTerm extends Expression {
    @Getter protected Value value;

    public LiteralTerm(String variableName, DataType dataType) {
        super(variableName, dataType);
    }

    @Override
    public String getPrintableExpression() {
        return getVariableName();
    }

    @Override
    public ExpressionEvaluator getEvaluator(Tuple sampleTuple, Graph graph) {
        return (Tuple tupleToEvaluator) -> value;
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
    public Set<PropertyVariable> getDependentPropertyVariables() {
        return new HashSet<>();
    }

    public abstract Object getLiteral();
}
