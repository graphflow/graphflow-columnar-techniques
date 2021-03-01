package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.ParserMethodReturnValue;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.DoubleLiteral;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.IntLiteral;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.LiteralTerm;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.flat.BoolVal;
import ca.waterloo.dsg.graphflow.util.datatype.ComparisonOperator;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import lombok.Setter;

/**
 * TODO: This class needs to be simplified with a rewrite of A+ Indexes code. Currently the A+
 *  indexes code allows WHERE clauses in the index definitions that are only conjunctive queries
 *  which consist of a list of {@link ComparisonExpression}s. This code currently requires knowing
 *  of a ComparisonType. Eventually a comparison expression can be something very complex,
 *  say containing sub-expressions and should not contain a ComparisonType as a field because
 *  the types might be quite complex. Instead if necessary it can provide
 *  isEdgePropertyAndLiteralComparison() like helper methods.
 */
public class ComparisonExpression extends AbstractBinaryOperatorExpression implements
    ParserMethodReturnValue {

    @Getter @Setter private ComparisonOperator comparisonOperator;

    public ComparisonExpression(ComparisonOperator comparisonOperator, Expression leftExpression,
        Expression rightExpression) {
        super(leftExpression, rightExpression);
        this.comparisonOperator = comparisonOperator;
        setVariableName(getPrintableExpression());
        makeRightOperandLiteralIfNecessary();
        ensureComparisonIsInclusive();
    }

    @Override
    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {
        leftExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        rightExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        if (leftExpression.getDataType() != rightExpression.getDataType()) {
            if (!(leftExpression.getDataType().isNumeric()) ||
                !(rightExpression.getDataType().isNumeric())) {
                var nodeIDCmp =
                    leftExpression.getDataType() == DataType.NODE && rightExpression.getDataType() == DataType.INT;
                if (!nodeIDCmp) {
                    throw new MalformedQueryException("Type error: Left and right data types are " +
                        "inconsistent in comparison expression: " + getPrintableExpression() + " " +
                        "Left data type: " + leftExpression.getDataType() + " right data type: " +
                        rightExpression.getDataType());
                }
            }
        }
        setDataType(DataType.BOOLEAN);
    }

    @Override
    public ExpressionEvaluator getEvaluator(Tuple sampleTuple, Graph graph) {
        var boolVal = new BoolVal(false);
        var leftExprEval = leftExpression.getEvaluator(sampleTuple, graph);
        var rightExprEval = rightExpression.getEvaluator(sampleTuple, graph);
        switch (leftExpression.getDataType()) {
            case BOOLEAN:
                switch (comparisonOperator) {
                    case EQUALS:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getBool()
                                == rightExprEval.evaluate(tupleToEvaluator).getBool());
                            return boolVal;
                        };
                    case NOT_EQUALS:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getBool()
                                != rightExprEval.evaluate(tupleToEvaluator).getBool());
                            return boolVal;
                        };
                    case AND:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getBool() &&
                                rightExprEval.evaluate(tupleToEvaluator).getBool());
                            return boolVal;
                        };
                    case OR:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getBool() ||
                                rightExprEval.evaluate(tupleToEvaluator).getBool());
                            return boolVal;
                        };
                    default:
                        throw new UnsupportedOperationException("Comparison operator " + comparisonOperator.name()
                            + " is not supported for booleans.");
                }
            case INT: case DOUBLE:
                switch (comparisonOperator) {
                    case EQUALS:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getDouble()
                                == rightExprEval.evaluate(tupleToEvaluator).getDouble());
                            return boolVal;
                        };
                    case GREATER_THAN:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getDouble() >
                                rightExprEval.evaluate(tupleToEvaluator).getDouble());
                            return boolVal;
                        };
                    case GREATER_THAN_OR_EQUAL:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getDouble()
                                >= rightExprEval.evaluate(tupleToEvaluator).getDouble());
                            return boolVal;
                        };
                    case LESS_THAN:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getDouble()
                                < rightExprEval.evaluate(tupleToEvaluator).getDouble());
                            return boolVal;
                        };
                    case LESS_THAN_OR_EQUAL:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getDouble()
                                <= rightExprEval.evaluate(tupleToEvaluator).getDouble());
                            return boolVal;
                        };
                    case NOT_EQUALS:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getDouble()
                                != rightExprEval.evaluate(tupleToEvaluator).getDouble());
                            return boolVal;
                        };
                    default:
                        throw new UnsupportedOperationException("Comparison operator " + comparisonOperator.name()
                            + " is not supported for double.");
                }
            case NODE:
                switch (comparisonOperator) {
                    case EQUALS:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getInt()
                                == rightExprEval.evaluate(tupleToEvaluator).getInt());
                            return boolVal;
                        };
                    case GREATER_THAN:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getInt() >
                                rightExprEval.evaluate(tupleToEvaluator).getInt());
                            return boolVal;
                        };
                    case GREATER_THAN_OR_EQUAL:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getInt()
                                >= rightExprEval.evaluate(tupleToEvaluator).getInt());
                            return boolVal;
                        };
                    case LESS_THAN:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getInt()
                                < rightExprEval.evaluate(tupleToEvaluator).getInt());
                            return boolVal;
                        };
                    case LESS_THAN_OR_EQUAL:
                        return (Tuple tupleToEvaluator) -> {
                            boolVal.setBool(leftExprEval.evaluate(tupleToEvaluator).getInt()
                                <= rightExprEval.evaluate(tupleToEvaluator).getInt());
                            return boolVal;
                        };
                    default:
                        throw new UnsupportedOperationException("Comparison operator " + comparisonOperator.name()
                            + " is not supported for double.");
                }
            case STRING:
                switch (comparisonOperator) {
                    case LESS_THAN:
                        return (Tuple tupleToEvaluator) -> {
                            var leftString = leftExprEval.evaluate(tupleToEvaluator);
                            var rightString = rightExprEval.evaluate(tupleToEvaluator);
                            boolVal.setBool(leftString.getString().compareTo(rightString.getString()) < 0);
                            return boolVal;
                        };
                    case LESS_THAN_OR_EQUAL:
                        return (Tuple tupleToEvaluator) -> {
                            var leftString = leftExprEval.evaluate(tupleToEvaluator);
                            var rightString = rightExprEval.evaluate(tupleToEvaluator);
                            boolVal.setBool(leftString.getString().compareTo(rightString.getString()) <= 0);
                            return boolVal;
                        };
                    case GREATER_THAN:
                        return (Tuple tupleToEvaluator) -> {
                            var leftString = leftExprEval.evaluate(tupleToEvaluator);
                            var rightString = rightExprEval.evaluate(tupleToEvaluator);
                            boolVal.setBool(leftString.getString().compareTo(rightString.getString()) > 0);
                            return boolVal;
                        };
                    case GREATER_THAN_OR_EQUAL:
                        return (Tuple tupleToEvaluator) -> {
                            var leftString = leftExprEval.evaluate(tupleToEvaluator);
                            var rightString = rightExprEval.evaluate(tupleToEvaluator);
                            boolVal.setBool(leftString.getString().compareTo(rightString.getString()) >= 0);
                            return boolVal;
                        };
                    case EQUALS:
                        return (Tuple tupleToEvaluator) -> {
                            var leftString = leftExprEval.evaluate(tupleToEvaluator);
                            var rightString = rightExprEval.evaluate(tupleToEvaluator);
                            boolVal.setBool(leftString.getString().equals(rightString.getString()));
                            return boolVal;
                        };
                    case NOT_EQUALS:
                        return (Tuple tupleToEvaluator) -> {
                            var leftString = leftExprEval.evaluate(tupleToEvaluator);
                            var rightString = rightExprEval.evaluate(tupleToEvaluator);
                            boolVal.setBool(!leftString.getString().equals(rightString.getString()));
                            return boolVal;
                        };
                    case STARTS_WITH:
                        return (Tuple tupleToEvaluator) -> {
                            var leftString = leftExprEval.evaluate(tupleToEvaluator);
                            var rightString = rightExprEval.evaluate(tupleToEvaluator);
                            boolVal.setBool(leftString.getString().startsWith(rightString.getString()));
                            return boolVal;
                        };
                    case ENDS_WITH:
                        return (Tuple tupleToEvaluator) -> {
                            var leftString = leftExprEval.evaluate(tupleToEvaluator);
                            var rightString = rightExprEval.evaluate(tupleToEvaluator);
                            boolVal.setBool(leftString.getString().endsWith(rightString.getString()));
                            return boolVal;
                        };
                    case CONTAINS:
                        return (Tuple tupleToEvaluator) -> {
                            var leftString = leftExprEval.evaluate(tupleToEvaluator);
                            var rightString = rightExprEval.evaluate(tupleToEvaluator);
                            boolVal.setBool(leftString.getString().contains(rightString.getString()));
                            return boolVal;
                        };
                    default:
                        throw new UnsupportedOperationException("Comparison operator " + comparisonOperator.name()
                            + " is not supported for Strings.");
                }
            default:
                throw new UnsupportedOperationException("We do not yet support comparing data " +
                    "type: " + leftExpression.getDataType().name() + " in comparison evaluators");
        }
    }

    public LiteralTerm getLiteralTerm() {
        if (!(rightExpression instanceof LiteralTerm)) {
            return null;
        }
        return ((LiteralTerm) rightExpression);
    }

    private void ensureComparisonIsInclusive() {
        if (!(rightExpression instanceof LiteralTerm)) {
            return;
        }
        var literalTerm = ((LiteralTerm) rightExpression);
        if (comparisonOperator == ComparisonOperator.LESS_THAN) {
            comparisonOperator = ComparisonOperator.LESS_THAN_OR_EQUAL;
            if (literalTerm instanceof IntLiteral) {
                var longLiteral = (IntLiteral) literalTerm;
                longLiteral.setNewLiteralValue(longLiteral.getIntLiteral() - 1);
            } else if (literalTerm instanceof DoubleLiteral) {
                var doubleLiteral = (DoubleLiteral) literalTerm;
                doubleLiteral.setNewLiteralValue(doubleLiteral.getDoubleLiteral() - Double.MIN_VALUE);
            }
        } else if (comparisonOperator == ComparisonOperator.GREATER_THAN) {
            comparisonOperator = ComparisonOperator.GREATER_THAN_OR_EQUAL;
            if (literalTerm instanceof IntLiteral) {
                var longLiteral = (IntLiteral) literalTerm;
                longLiteral.setNewLiteralValue(longLiteral.getIntLiteral() + 1);
            } else if (literalTerm instanceof DoubleLiteral) {
                var doubleLiteral = (DoubleLiteral) literalTerm;
                doubleLiteral.setNewLiteralValue(doubleLiteral.getDoubleLiteral() + Double.MIN_VALUE);
            }
        }
    }

    private void makeRightOperandLiteralIfNecessary() {
        if (leftExpression instanceof LiteralTerm) {
            var tmp = rightExpression;
            rightExpression = leftExpression;
            leftExpression = tmp;
            comparisonOperator = comparisonOperator.getReverseOperator();
        }
    }

    @Override
    public String toString() {
        var stringBuilder = new StringBuilder();
        stringBuilder.append(leftExpression.getVariableName());
        stringBuilder.append(" ").append(comparisonOperator.toString()).append(" ");
        if (null != rightExpression) {
            stringBuilder.append(rightExpression.getVariableName());
        }
        return stringBuilder.toString();
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        switch (comparisonOperator) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return constructReverse().hashCode();
        }
        hash = 31*hash + comparisonOperator.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        switch (comparisonOperator) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return constructReverse().equals(o);
        }
        var other = (ComparisonExpression) o;
        return leftExpression.equals(other.leftExpression) &&
            rightExpression.equals(other.rightExpression) &&
            comparisonOperator == other.comparisonOperator;
    }

    private ComparisonExpression constructReverse() {
        return new ComparisonExpression(
            comparisonOperator.getReverseOperator(), rightExpression, leftExpression);
    }

    @Override
    public String getPrintableExpression() {
        return leftExpression.getPrintableExpression() + " " + comparisonOperator.getSymbol() + " "
            + rightExpression.getPrintableExpression();
    }
}
