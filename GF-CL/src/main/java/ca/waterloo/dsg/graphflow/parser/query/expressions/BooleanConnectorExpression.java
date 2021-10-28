package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorState;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.util.StringJoiner;

public abstract class BooleanConnectorExpression extends BinaryOperatorExpression {

    public enum BooleanConnector {
        AND,
        OR
    }

    @Getter private final BooleanConnector boolConnector;

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
        public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
            var lEvaluator = leftExpression.getEvaluator(dataChunks);
            var rEvaluator = rightExpression.getEvaluator(dataChunks);
            var lResult = leftExpression.result;
            var rResult = rightExpression.result;
            var lBools = lResult.getBooleans();
            var rBools = rResult.getBooleans();
            var isLFlat = ExpressionUtils.isExpressionOutputFlat(leftExpression, dataChunks);
            var isRFlat = ExpressionUtils.isExpressionOutputFlat(rightExpression, dataChunks);
            var capacity = isLFlat && isRFlat ? 1 : Vector.DEFAULT_VECTOR_SIZE;
            result =  Vector.make(DataType.BOOLEAN, capacity);
            result.state = isLFlat && isRFlat ? VectorState.getFlatVectorState() : (
                isLFlat ? rResult.state : lResult.state);
            var results = result.getBooleans();
            if (isLFlat && isRFlat) {
                return () -> {
                    lEvaluator.evaluate();
                    rEvaluator.evaluate();
                    results[0] =
                        (lBools[lResult.state.getCurrSelectedValuesPos()] &&
                            rBools[rResult.state.getCurrSelectedValuesPos()]);
                };
            } else if (!isLFlat && isRFlat) {
                return () -> {
                    lEvaluator.evaluate();
                    rEvaluator.evaluate();
                    var rVal = rBools[rResult.state.getCurrSelectedValuesPos()];
                    for (var i = 0; i < lResult.state.size; i++) {
                        var pos = lResult.state.selectedValuesPos[i];
                        results[pos] = (lBools[pos] && rVal);
                    }
                };
            } else if (isLFlat) {
                return () -> {
                    lEvaluator.evaluate();
                    rEvaluator.evaluate();
                    var lVal = lBools[lResult.state.getCurrSelectedValuesPos()];
                    for (var i = 0; i < rResult.state.size; i++) {
                        var pos = rResult.state.selectedValuesPos[i];
                        results[pos] = (lVal && rBools[pos]);
                    }
                };
            } else {
                return () -> {
                    lEvaluator.evaluate();
                    rEvaluator.evaluate();
                    for (var i = 0; i < result.state.size; i++) {
                        var pos = result.state.getCurrSelectedValuesPos();
                        results[pos] = (lBools[pos] && rBools[pos]);
                    }
                };
            }
        }
    }

    public static class ORExpression extends BooleanConnectorExpression {

        public ORExpression(Expression leftExpression, Expression rightExpression) {
            super(leftExpression, rightExpression, BooleanConnector.OR);
        }

        @Override
        public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
            var lEvaluator = leftExpression.getEvaluator(dataChunks);
            var rEvaluator = rightExpression.getEvaluator(dataChunks);
            var lResult = leftExpression.result;
            var rResult = rightExpression.result;
            var lBools = lResult.getBooleans();
            var rBools = rResult.getBooleans();
            var isLFlat = ExpressionUtils.isExpressionOutputFlat(leftExpression, dataChunks);
            var isRFlat = ExpressionUtils.isExpressionOutputFlat(rightExpression, dataChunks);
            var capacity = isLFlat && isRFlat ? 1 : Vector.DEFAULT_VECTOR_SIZE;
            result =  Vector.make(DataType.BOOLEAN, capacity);
            result.state = isLFlat && isRFlat ? VectorState.getFlatVectorState() : (
                isLFlat ? rResult.state : lResult.state);
            var results = result.getBooleans();
            if (isLFlat && isRFlat) {
                return () -> {
                    lEvaluator.evaluate();
                    rEvaluator.evaluate();
                    results[0] =
                        (lBools[lResult.state.getCurrSelectedValuesPos()] ||
                            rBools[rResult.state.getCurrSelectedValuesPos()]);
                };
            } else if (!isLFlat && isRFlat) {
                return () -> {
                    lEvaluator.evaluate();
                    rEvaluator.evaluate();
                    var rVal = rBools[rResult.state.getCurrSelectedValuesPos()];
                    for (var i = 0; i < lResult.state.size; i++) {
                        var pos = lResult.state.selectedValuesPos[i];
                        results[pos] = (lBools[pos] || rVal);
                    }
                };
            } else if (isLFlat) {
                return () -> {
                    lEvaluator.evaluate();
                    rEvaluator.evaluate();
                    var lVal = lBools[lResult.state.getCurrSelectedValuesPos()];
                    for (var i = 0; i < rResult.state.size; i++) {
                        var pos = rResult.state.selectedValuesPos[i];
                        results[pos] = (lVal || rBools[pos]);
                    }
                };
            } else {
                return () -> {
                    lEvaluator.evaluate();
                    rEvaluator.evaluate();
                    for (var i = 0; i < result.state.size; i++) {
                        var pos = result.state.getCurrSelectedValuesPos();
                        results[pos] = (lBools[pos] || rBools[pos]);
                    }
                };
            }
        }
    }
}
