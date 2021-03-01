package ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import lombok.Getter;

import java.io.Serializable;

public abstract class AbstractExpressionEvaluatingOperator extends Operator implements Serializable {
    @Getter
    protected Expression expression;

    protected transient ExpressionEvaluator evaluator;

    public AbstractExpressionEvaluatingOperator(Expression expression) {
        this.expression = expression;
    }
}
