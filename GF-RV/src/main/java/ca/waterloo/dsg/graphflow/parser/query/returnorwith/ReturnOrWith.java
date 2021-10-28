package ca.waterloo.dsg.graphflow.parser.query.returnorwith;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.query.OrderByConstraint;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

public abstract class ReturnOrWith {

    @Getter private ReturnBody returnBody;

    public ReturnOrWith(ReturnBody returnBody) {
        this.returnBody = returnBody;
    }

    public static class Return extends ReturnOrWith {

        public Return(ReturnBody returnBody) {
            super(returnBody);
        }

    }

    public static class WithWhere extends ReturnOrWith {

        @Getter @Setter private Expression whereExpression;

        public WithWhere(ReturnBody returnBody) {
            super(returnBody);
        }

        public boolean hasWhereExpression() {
            return null != whereExpression;
        }
    }

    public static class ReturnBody {

        public enum ReturnBodyType {
            RETURN_OR_WITH_STAR,
            GROUP_BY_AND_AGGREGATE,
            PROJECTION;
        }

        @Getter private ReturnBodyType returnBodyType;

        @Getter private GroupByAndAggregateExpressions groupByAndAggregateExpressions;

        @Getter private ProjectionExpressions projectionExpressions;

        @Getter @Setter List<OrderByConstraint> orderByConstraints = new ArrayList<>();


        @Getter @Setter private long numTuplesToSkip = -1;

        @Getter @Setter private long numTuplesToLimit = -1;

        @Getter private Schema outputSchema;

        public ReturnBody(ReturnBodyType returnBodyType) {
            this.returnBodyType = returnBodyType;
            if (ReturnBodyType.GROUP_BY_AND_AGGREGATE == this.returnBodyType) {
                this.groupByAndAggregateExpressions = new GroupByAndAggregateExpressions();
            } else {
                this.projectionExpressions = new ProjectionExpressions();
            }
            this.outputSchema = new Schema();
        }

        public void addNonAggregatingExpression(Expression expression) {
            if (ReturnBodyType.GROUP_BY_AND_AGGREGATE == returnBodyType) {
                groupByAndAggregateExpressions.getGroupByExpressions().add(expression);
            } else {
                projectionExpressions.projectionExpressions.add(expression);
            }
            if (!outputSchema.containsVarName(expression.getVariableName())) {
                outputSchema.add(expression.getVariableName(), expression);
            }
        }

        public void addAggregatingExpression(Expression aggregatingExpression) {
            if (ReturnBodyType.GROUP_BY_AND_AGGREGATE != returnBodyType) {
                throw new MalformedQueryException("This should never happen! Aggregations for " +
                    returnBodyType.name() + " return bodies is not allowed.");
            }
            var variableName = aggregatingExpression.getVariableName();
            if (!outputSchema.containsVarName(variableName)) {
                groupByAndAggregateExpressions.aggregationExpressions.add(aggregatingExpression);
                outputSchema.add(variableName, aggregatingExpression);
            }
        }

        public boolean isSame(ReturnBody other) {
            if (this == other) {
                return true;
            }
            if (null == other) {
                return false;
            }
            if (this.returnBodyType != other.returnBodyType) {
                return false;
            }
            if (null == this.groupByAndAggregateExpressions && null != other.groupByAndAggregateExpressions ||
                null != this.groupByAndAggregateExpressions && null == other.groupByAndAggregateExpressions) {
                return false;
            }
            if (null != this.groupByAndAggregateExpressions && null != other.groupByAndAggregateExpressions) {
                if (!this.groupByAndAggregateExpressions.isSame(other.groupByAndAggregateExpressions)) {
                    return false;
                }
            }
            if (null == this.projectionExpressions && null != other.projectionExpressions ||
                null != this.projectionExpressions && null == other.projectionExpressions) {
                return false;
            }
            if (null != this.projectionExpressions && null != other.projectionExpressions) {
                if (!this.projectionExpressions.isSame(other.projectionExpressions)) {
                    return false;
                }
            }
            return this.outputSchema.isSame(other.outputSchema);
        }

        private static boolean isExpressionsSame(List<Expression> thisExpressions,
            List<Expression> otherExpressions) {
            List<Expression> otherExprs = new ArrayList<>(otherExpressions);
            for (Expression thisExpression : thisExpressions) {
                var i = 0;
                while (i < otherExprs.size()) {
                    if (thisExpression.equals(otherExprs.get(i))) {
                        break;
                    }
                    i++;
                }
                if (i < otherExprs.size()) {
                    otherExprs.remove(i);
                } else {
                    return false;
                }
            }
            return true;
        }

        public static class GroupByAndAggregateExpressions {
            @Getter @Setter
            private List<Expression> groupByExpressions = new ArrayList<>();
            @Getter @Setter
            private List<Expression> aggregationExpressions = new ArrayList<>();

            public boolean hasGroupByVariables() {
                return !groupByExpressions.isEmpty();
            }

            public List<Expression> getExpressions() {
                var retVal = new ArrayList<Expression>();
                groupByExpressions.forEach(expr ->{retVal.add(expr);});
                aggregationExpressions.forEach(expr ->{retVal.add(expr);});
                return retVal;
            }

            public boolean isSame(GroupByAndAggregateExpressions other) {
                if (this.groupByExpressions.size() != other.groupByExpressions.size()) {
                    return false;
                }
                if (this.aggregationExpressions.size() != other.aggregationExpressions.size()) {
                    return false;
                }
                if (!isExpressionsSame(groupByExpressions, other.groupByExpressions)) {
                    return false;
                }
                if (!isExpressionsSame(aggregationExpressions, other.aggregationExpressions)) {
                    return false;
                }
                return true;
            }
        }

        public static class ProjectionExpressions {

            @Getter private List<Expression> projectionExpressions = new ArrayList<>();

            public boolean isSame(ProjectionExpressions other) {
                if (this.projectionExpressions.size() != other.projectionExpressions.size()) {
                    return false;
                }
                return isExpressionsSame(this.projectionExpressions, other.projectionExpressions);
            }
        }
    }
}
