package ca.waterloo.dsg.graphflow.parser.query;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import lombok.Getter;
import lombok.Setter;

public class OrderByConstraint {
    @Getter @Setter Expression expression;
    @Getter @Setter OrderType orderType;

    public OrderByConstraint(Expression expression, OrderType orderType) {
        this.expression = expression;
        this.orderType = orderType;
    }

    public enum OrderType {
        ASCENDING,
        DESCENDING
    }
}
