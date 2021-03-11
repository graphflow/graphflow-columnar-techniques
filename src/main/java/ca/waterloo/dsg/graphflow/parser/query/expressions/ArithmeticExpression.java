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

public class ArithmeticExpression extends BinaryOperatorExpression {

    public enum ArithmeticOperator {
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MODULO("%"),
        POWER("^");

        private String symbol;
        ArithmeticOperator(String symbol) {
            this.symbol = symbol;
        }
    }

    @Getter private ArithmeticOperator arithmeticOperator;

    public ArithmeticExpression(ArithmeticOperator arithmeticOperator, Expression leftExpression,
        Expression rightExpression) {
        super(leftExpression, rightExpression);
        this.arithmeticOperator = arithmeticOperator;
        setVariableName(getPrintableExpression());
    }

    @Override
    public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
        var lEvaluator = leftExpression.getEvaluator(dataChunks);
        var rEvaluator = rightExpression.getEvaluator(dataChunks);
        var lResult = leftExpression.result;
        var rResult = rightExpression.result;
        var isLFlat = ExpressionUtils.isExpressionOutputFlat(leftExpression, dataChunks);
        var isRFlat = ExpressionUtils.isExpressionOutputFlat(rightExpression, dataChunks);
        var capacity = isLFlat && isRFlat ? 1 : Vector.DEFAULT_VECTOR_SIZE;
        switch (leftExpression.getDataType()) {
            case INT:
                var lInts = lResult.getInts();
                switch (rightExpression.getDataType()) {
                    case INT:
                        var rInts = rResult.getInts();
                        result =  Vector.make(DataType.INT, capacity);
                        result.state = isLFlat && isRFlat ? VectorState.getFlatVectorState() : (
                            isLFlat ? rResult.state : lResult.state);
                        var results = result.getInts();
                        switch (arithmeticOperator) {
                            case ADD:
                            if (isLFlat && isRFlat) {
                                return () -> {
                                    lEvaluator.evaluate();
                                    rEvaluator.evaluate();
                                    results[0] =
                                        (lInts[lResult.state.getCurrSelectedValuesPos()] +
                                            rInts[rResult.state.getCurrSelectedValuesPos()]);
                                };
                            } else if (!isLFlat && isRFlat) {
                                return () -> {
                                    lEvaluator.evaluate();
                                    rEvaluator.evaluate();
                                    var rVal = rInts[rResult.state.getCurrSelectedValuesPos()];
                                    for (var i = 0; i < lResult.state.size; i++) {
                                        var pos = lResult.state.selectedValuesPos[i];
                                        results[pos] = (lInts[pos] + rVal);
                                    }
                                };
                            } else if (isLFlat) {
                                return () -> {
                                    lEvaluator.evaluate();
                                    rEvaluator.evaluate();
                                    var lVal = lInts[lResult.state.getCurrSelectedValuesPos()];
                                    for (var i = 0; i < rResult.state.size; i++) {
                                        var pos = rResult.state.selectedValuesPos[i];
                                        results[pos] = (lVal + rInts[pos]);
                                    }
                                };
                            } else {
                                return () -> {
                                    lEvaluator.evaluate();
                                    rEvaluator.evaluate();
                                    for (var i = 0; i < result.state.size; i++) {
                                        var pos = result.state.getCurrSelectedValuesPos();
                                        results[pos] = (lInts[pos] + rInts[pos]);
                                    }
                                };
                            }
                            default:
                            throw new UnsupportedOperationException("Arithmetic operator " +
                                arithmeticOperator.name() + " is not supported for data types.");
                        }
                    default:
                        throw new UnsupportedOperationException("Arithmetic operator " +
                            arithmeticOperator.name() + " is not supported for data types.");
                }
            default:
                throw new UnsupportedOperationException("We do not yet support arithmetic " +
                    "operations: " + arithmeticOperator.name());
        }
    }

    @Override
    public String getPrintableExpression() {
        return leftExpression.getPrintableExpression() + " " + arithmeticOperator.symbol + " "
            + rightExpression.getPrintableExpression();
    }

    @Override
    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {
        leftExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        rightExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        checkExpressionIsNumericDataType(leftExpression);
        checkExpressionIsNumericDataType(rightExpression);
        if (DataType.DOUBLE == leftExpression.getDataType() ||
            DataType.DOUBLE == rightExpression.getDataType()) {
            setDataType(DataType.DOUBLE);
        } else  if (DataType.INT == leftExpression.getDataType() ||
            DataType.INT == rightExpression.getDataType()) {
            if (ArithmeticOperator.DIVIDE == arithmeticOperator) {
                setDataType(DataType.DOUBLE);
            } else {
                setDataType(DataType.INT);
            }
        } else { // Both data types must be ints;
            if (ArithmeticOperator.DIVIDE == arithmeticOperator) {
                setDataType(DataType.DOUBLE);
            } else {
                setDataType(DataType.INT);
            }
        }
    }

    private void checkExpressionIsNumericDataType(Expression expression) {
        if (DataType.DOUBLE != expression.getDataType() &&
            DataType.INT != expression.getDataType()) {
            throw new MalformedQueryException("An operand, i.e., left or right expression, in an " +
                "arithmethic expression is not a numeric type: " + leftExpression.getDataType());
        }
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        hash = 31*hash + arithmeticOperator.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherArithmeticExpr = (ArithmeticExpression) o;
        return this.arithmeticOperator == otherArithmeticExpr.arithmeticOperator &&
            this.leftExpression.equals(otherArithmeticExpr.leftExpression) &&
            this.rightExpression.equals(otherArithmeticExpr.rightExpression);
    }
}
