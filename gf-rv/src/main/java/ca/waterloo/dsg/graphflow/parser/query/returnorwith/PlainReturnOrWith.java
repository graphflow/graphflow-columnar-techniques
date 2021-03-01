package ca.waterloo.dsg.graphflow.parser.query.returnorwith;

import ca.waterloo.dsg.graphflow.parser.ParserMethodReturnValue;
import ca.waterloo.dsg.graphflow.parser.query.OrderByConstraint;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class PlainReturnOrWith {

    @Getter private PlainReturnBody plainReturnBody;

    public PlainReturnOrWith(PlainReturnBody plainReturnBody) {
        this.plainReturnBody = plainReturnBody;
    }

    public static class PlainReturn extends PlainReturnOrWith {

        public PlainReturn(PlainReturnBody plainReturnBody) {
            super(plainReturnBody);
        }
    }

    public static class PlainWith extends PlainReturnOrWith {

        @Getter @Setter private Expression whereExpression;

        public boolean hasWhereExpression() {
            return null != whereExpression;
        }

        public PlainWith(PlainReturnBody plainReturnBody) {
            super(plainReturnBody);
        }
    }

    public static class PlainReturnBody implements ParserMethodReturnValue {

        @Getter List<Expression> expressions = new ArrayList<>();

        @Getter @Setter long numTuplesToSkip = -1;

        @Getter @Setter long numTuplesToLimit = -1;

        @Getter List<OrderByConstraint> orderByConstraints = new ArrayList<>();

        @Getter @Setter boolean isReturnStar = false;

        public void addExpression(Expression expression) {
            expressions.add(expression);
        }

        public boolean containsAggregatingExpression() {
            for (var expr : expressions) {
                if (expr.hasDependentFunctionInvocations()) {
                    return true;
                }
            }
            return false;
        }

        public List<Expression> getAggregatingExpressions() {
            return expressions.stream().filter(Expression::hasDependentFunctionInvocations)
                .collect(Collectors.toList());
        }

        public List<Expression> getNonAggregatingExpressions() {
            return expressions.stream().filter(expression ->
                !expression.hasDependentFunctionInvocations()).collect(Collectors.toList());
        }

        public void addOrderByConstraints(OrderByConstraint orderByConstraint) {
            orderByConstraints.add(orderByConstraint);
        }
    }
}
