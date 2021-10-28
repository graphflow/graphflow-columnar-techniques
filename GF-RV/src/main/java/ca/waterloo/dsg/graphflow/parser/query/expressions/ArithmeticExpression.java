package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.flat.DoubleVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

public class ArithmeticExpression extends AbstractBinaryOperatorExpression {

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
    public ExpressionEvaluator getEvaluator(Tuple sampleTuple, Graph graph) {
        var leftExprEval = leftExpression.getEvaluator(sampleTuple, graph);
        var rightExprEval = rightExpression.getEvaluator(sampleTuple, graph);
        switch (dataType) {
            case INT :
                var longVal = new IntVal(DataType.NULL_INTEGER);
                switch (arithmeticOperator) {
                    case ADD:
                        return (Tuple tupleToEvaluator) -> {
                            longVal.setInt(leftExprEval.evaluate(tupleToEvaluator).getInt()
                                + rightExprEval.evaluate(tupleToEvaluator).getInt());
                            return longVal;
                        };
                    case SUBTRACT:
                        return (Tuple tupleToEvaluator) -> {
                            longVal.setInt(leftExprEval.evaluate(tupleToEvaluator).getInt()
                                - rightExprEval.evaluate(tupleToEvaluator).getInt());
                            return longVal;
                        };
                    case MULTIPLY:
                        return (Tuple tupleToEvaluator) -> {
                            longVal.setInt(leftExprEval.evaluate(tupleToEvaluator).getInt()
                                * rightExprEval.evaluate(tupleToEvaluator).getInt());
                            return longVal;
                        };
                    case MODULO:
                        return (Tuple tupleToEvaluator) -> {
                            longVal.setInt(leftExprEval.evaluate(tupleToEvaluator).getInt()
                                % rightExprEval.evaluate(tupleToEvaluator).getInt());
                            return longVal;
                        };
                    case POWER:
                        return (Tuple tupleToEvaluator) -> {
                            longVal.setInt((int) Math.pow(
                                leftExprEval.evaluate(tupleToEvaluator).getInt(),
                                rightExprEval.evaluate(tupleToEvaluator).getInt()));
                            return longVal;
                        };
                    default:
                        throw new UnsupportedOperationException("Arithmetic operator " +
                            arithmeticOperator.name() + " is not supported in expressions.");
                }
            case DOUBLE:
                var doubleVal = new DoubleVal(DataType.NULL_DOUBLE);
                switch (arithmeticOperator) {
                    case ADD:
                        return (Tuple tupleToEvaluator) -> {
                            doubleVal.setDouble(leftExprEval.evaluate(tupleToEvaluator).getDouble()
                                + rightExprEval.evaluate(tupleToEvaluator).getDouble());
                            return doubleVal;
                        };
                    case SUBTRACT:
                        return (Tuple tupleToEvaluator) -> {
                            doubleVal.setDouble(leftExprEval.evaluate(tupleToEvaluator).getDouble()
                                - rightExprEval.evaluate(tupleToEvaluator).getDouble());
                            return doubleVal;
                        };
                    case DIVIDE:
                        return (Tuple tupleToEvaluator) -> {
                            doubleVal.setDouble(leftExprEval.evaluate(tupleToEvaluator).getDouble()
                                / rightExprEval.evaluate(tupleToEvaluator).getDouble());
                            return doubleVal;
                        };
                    case MULTIPLY:
                        return (Tuple tupleToEvaluator) -> {
                            doubleVal.setDouble(leftExprEval.evaluate(tupleToEvaluator).getDouble()
                                * rightExprEval.evaluate(tupleToEvaluator).getDouble());
                            return doubleVal;
                        };
                    case MODULO:
                        return (Tuple tupleToEvaluator) -> {
                            doubleVal.setDouble(leftExprEval.evaluate(tupleToEvaluator).getDouble()
                                % rightExprEval.evaluate(tupleToEvaluator).getDouble());
                            return doubleVal;
                        };
                    case POWER:
                        return (Tuple tupleToEvaluator) -> {
                            doubleVal.setDouble(Math.pow(
                                leftExprEval.evaluate(tupleToEvaluator).getDouble(),
                                rightExprEval.evaluate(tupleToEvaluator).getDouble()));
                            return doubleVal;
                        };
                    default:
                        throw new UnsupportedOperationException("Arithmetic operator " +
                            arithmeticOperator.name() + " is not yet supported in expressions.");
                }
        }
        throw new UnsupportedOperationException("We do not support arithmetic expressions " +
            " on: " + dataType.name() + ".");
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
