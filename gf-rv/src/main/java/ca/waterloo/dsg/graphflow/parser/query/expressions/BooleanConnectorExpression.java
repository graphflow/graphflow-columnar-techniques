package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.flat.BoolVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.util.StringJoiner;

public abstract class BooleanConnectorExpression extends AbstractBinaryOperatorExpression {

    private enum BooleanConnector {
        AND,
        OR
    }

    private BooleanConnector boolConnector;

    public BooleanConnectorExpression(Expression leftExpression,
        Expression rightExpression, BooleanConnector boolConnector) {
        super("");
        this.leftExpression = leftExpression;
        this.rightExpression = rightExpression;
        this.boolConnector = boolConnector;
        setVariableName(getPrintableExpression());
    }

    @Override
    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {
        leftExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        throwExceptionIfSubexpressionDoesNotReturnABoolean(leftExpression);
        rightExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        throwExceptionIfSubexpressionDoesNotReturnABoolean(rightExpression);
        setDataType(DataType.BOOLEAN);
    }

    private void throwExceptionIfSubexpressionDoesNotReturnABoolean(Expression subExpression) {
        if (DataType.BOOLEAN != subExpression.getDataType()) {
            throw new MalformedQueryException("Sub expression: "
                + subExpression.getPrintableExpression() + " of " + getClass().getSimpleName() +
                " has to return a BOOLEAN. Return type of subExpression: "
                + subExpression.getDataType()) ;
        }
    }

    @Override
    public String getPrintableExpression() {
        StringJoiner stringJoiner = new StringJoiner("");
        stringJoiner.add(leftExpression.getPrintableExpression());
        stringJoiner.add(" " + boolConnector.name() + " ");
        stringJoiner.add(rightExpression.getPrintableExpression());
        return stringJoiner.toString();
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        hash = 31*hash + boolConnector.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherBoolConExpr = (BooleanConnectorExpression) o;
        if (this.boolConnector != otherBoolConExpr.boolConnector) {
            return false;
        }
        if ((leftExpression.equals(otherBoolConExpr.leftExpression) &&
            rightExpression.equals(otherBoolConExpr.rightExpression)) ||
            (leftExpression.equals(otherBoolConExpr.rightExpression) &&
                rightExpression.equals(otherBoolConExpr.leftExpression))) {
            return true;
        }
        return false;
    }

    public static class ANDExpression extends BooleanConnectorExpression {

        public ANDExpression(Expression leftExpression, Expression rightExpression) {
            super(leftExpression, rightExpression, BooleanConnector.AND);
        }

        @Override
        public ExpressionEvaluator getEvaluator(Tuple sampleTuple, Graph graph) {
            var leftEvaluator = leftExpression.getEvaluator(sampleTuple, graph);
            var rightEvaluator = rightExpression.getEvaluator(sampleTuple, graph);
            return (Tuple tupleToEvaluator) -> {
                if (leftEvaluator.evaluate(tupleToEvaluator).getBool() &&
                    rightEvaluator.evaluate(tupleToEvaluator).getBool()) {
                    return BoolVal.TRUE_BOOL_VAL;
                }
                return BoolVal.FALSE_BOOL_VAL;
            };
        }
    }

    public static class ORExpression extends BooleanConnectorExpression {

        public ORExpression(Expression leftExpression, Expression rightExpression) {
            super(leftExpression, rightExpression, BooleanConnector.OR);
        }

        @Override
        public ExpressionEvaluator getEvaluator(Tuple sampleTuple, Graph graph) {
            var leftEvaluator = leftExpression.getEvaluator(sampleTuple, graph);
            var rightEvaluator = rightExpression.getEvaluator(sampleTuple, graph);
            return (Tuple tupleToEvaluator) -> {
                if (leftEvaluator.evaluate(tupleToEvaluator).getBool() ||
                    rightEvaluator.evaluate(tupleToEvaluator).getBool()) {
                    return BoolVal.TRUE_BOOL_VAL;
                }
                return BoolVal.FALSE_BOOL_VAL;
            };
        }
    }
}
