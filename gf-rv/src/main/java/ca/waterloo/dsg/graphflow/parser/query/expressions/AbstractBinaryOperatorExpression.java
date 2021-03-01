package ca.waterloo.dsg.graphflow.parser.query.expressions;

import lombok.Getter;
import lombok.Setter;

import java.util.Set;

public abstract class AbstractBinaryOperatorExpression extends Expression {
    @Getter @Setter Expression leftExpression = null;
    @Getter @Setter Expression rightExpression = null;

    public AbstractBinaryOperatorExpression(String variableName) {
        super(variableName);
    }

    public AbstractBinaryOperatorExpression(Expression leftExpression, Expression rightExpression) {
        super("");
        this.leftExpression = leftExpression;
        this.rightExpression = rightExpression;
    }

    @Override
    public Set<String> getDependentVariableNames() {
        return ExpressionUtils.getUnionedDependentVariables(leftExpression, rightExpression);
    }

    @Override
    public Set<String> getDependentExpressionVariableNames() {
        return ExpressionUtils.getUnionedDependentExpressionVariables(leftExpression,
            rightExpression);
    }

    @Override
    public Set<PropertyVariable> getDependentPropertyVariables() {
        return ExpressionUtils.getUnionedDependentPropertyVariables(leftExpression,
            rightExpression);
    }

    @Override
    public Set<FunctionInvocation> getDependentFunctionInvocations() {
        return ExpressionUtils.getUnionedDependentFunctionInvocations(leftExpression,
            rightExpression);
    }

    @Override
    public boolean hasDependentFunctionInvocations() {
        return leftExpression.hasDependentFunctionInvocations() || rightExpression.hasDependentFunctionInvocations();
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        hash = 31*hash + leftExpression.hashCode();
        hash = 31*hash + rightExpression.hashCode();
        return hash;
    }
}
