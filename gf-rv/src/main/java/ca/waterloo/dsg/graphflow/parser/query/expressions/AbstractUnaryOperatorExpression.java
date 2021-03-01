package ca.waterloo.dsg.graphflow.parser.query.expressions;

import lombok.Getter;

import java.util.Set;

public abstract class AbstractUnaryOperatorExpression extends Expression {

    @Getter protected Expression expression;

    public AbstractUnaryOperatorExpression(String variableName, Expression expression) {
        super(variableName);
        this.expression = expression;
    }

    @Override
    public Set<String> getDependentVariableNames() {
        return expression.getDependentVariableNames();
    }

    @Override
    public Set<String> getDependentExpressionVariableNames() {
        return expression.getDependentExpressionVariableNames();
    }

    @Override
    public Set<PropertyVariable> getDependentPropertyVariables() {
        return expression.getDependentPropertyVariables();
    }

    @Override
    public Set<FunctionInvocation> getDependentFunctionInvocations() {
        return expression.getDependentFunctionInvocations();
    }

    @Override
    public boolean hasDependentFunctionInvocations() {
        return expression.hasDependentFunctionInvocations();
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        hash = 31*hash + expression.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var other = (AbstractUnaryOperatorExpression) o;
        return expression.equals(other.expression);
    }
}
