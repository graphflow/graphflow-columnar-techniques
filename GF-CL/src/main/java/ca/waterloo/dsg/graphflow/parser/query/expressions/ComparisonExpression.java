package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorState;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.ParserMethodReturnValue;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.DoubleLiteral;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.IntLiteral;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.LiteralTerm;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.ComparisonOperator;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import lombok.Setter;

public class ComparisonExpression extends BinaryOperatorExpression implements
    ParserMethodReturnValue {

    @Getter
    @Setter
    private ComparisonOperator comparisonOperator;

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
                throw new MalformedQueryException("Type error: Left and right data types are " +
                    "inconsistent in comparison expression: " + getPrintableExpression() + " " +
                    "Left data type: " + leftExpression.getDataType() + " right data type: " +
                    rightExpression.getDataType());
            }
        }
        setDataType(DataType.BOOLEAN);
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
        result = Vector.make(DataType.BOOLEAN, capacity);
        result.state = isLFlat && isRFlat ? VectorState.getFlatVectorState() : (
            isLFlat ? rResult.state : lResult.state);
        var results = result.getBooleans();
        switch (leftExpression.getDataType()) {
            case INT:
                var lInts = lResult.getInts();
                switch (rightExpression.getDataType()) {
                    case INT:
                        var rInts = rResult.getInts();
                        switch (comparisonOperator) {
                            case EQUALS:
                                if (isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        results[0] =
                                            (lInts[lResult.state.getCurrSelectedValuesPos()] ==
                                                rInts[rResult.state.getCurrSelectedValuesPos()]);
                                    };
                                } else if (!isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var rVal = rInts[rResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < lResult.state.size; i++) {
                                            var pos = lResult.state.selectedValuesPos[i];
                                            results[pos] = (lInts[pos] == rVal);
                                        }
                                    };
                                } else if (isLFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var lVal = lInts[lResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < rResult.state.size; i++) {
                                            var pos = rResult.state.selectedValuesPos[i];
                                            results[pos] = (lVal == rInts[pos]);
                                        }
                                    };
                                } else {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        for (var i = 0; i < result.state.size; i++) {
                                            var pos = result.state.getCurrSelectedValuesPos();
                                            results[pos] = (lInts[pos] == rInts[pos]);
                                        }
                                    };
                                }
                            case NOT_EQUALS:
                                if (isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        results[0] =
                                            (lInts[lResult.state.getCurrSelectedValuesPos()] !=
                                                rInts[rResult.state.getCurrSelectedValuesPos()]);
                                    };
                                } else if (!isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var rVal = rInts[rResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < lResult.state.size; i++) {
                                            var pos = lResult.state.selectedValuesPos[i];
                                            results[pos] = (lInts[pos] != rVal);
                                        }
                                    };
                                } else if (isLFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var lVal = lInts[lResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < rResult.state.size; i++) {
                                            var pos = rResult.state.selectedValuesPos[i];
                                            results[pos] = (lVal != rInts[pos]);
                                        }
                                    };
                                } else {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        for (var i = 0; i < result.state.size; i++) {
                                            var pos = result.state.getCurrSelectedValuesPos();
                                            results[pos] = (lInts[pos] != rInts[pos]);
                                        }
                                    };
                                }
                            case GREATER_THAN_OR_EQUAL:
                                if (isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        results[0] =
                                            (lInts[lResult.state.getCurrSelectedValuesPos()] >=
                                                rInts[rResult.state.getCurrSelectedValuesPos()]);
                                    };
                                } else if (!isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var rVal = rInts[rResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < lResult.state.size; i++) {
                                            var pos = lResult.state.selectedValuesPos[i];
                                            results[pos] = (lInts[pos] >= rVal);
                                        }
                                    };
                                } else if (isLFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var lVal = lInts[lResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < rResult.state.size; i++) {
                                            var pos = rResult.state.selectedValuesPos[i];
                                            results[pos] = (lVal >= rInts[pos]);
                                        }
                                    };
                                } else {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        for (var i = 0; i < result.state.size; i++) {
                                            var pos = result.state.getCurrSelectedValuesPos();
                                            results[pos] = (lInts[pos] >= rInts[pos]);
                                        }
                                    };
                                }
                            case GREATER_THAN:
                                if (isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        results[0] =
                                            (lInts[lResult.state.getCurrSelectedValuesPos()] >
                                                rInts[rResult.state.getCurrSelectedValuesPos()]);
                                    };
                                } else if (!isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var rVal = rInts[rResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < lResult.state.size; i++) {
                                            var pos = lResult.state.selectedValuesPos[i];
                                            results[pos] = (lInts[pos] > rVal);
                                        }
                                    };
                                } else if (isLFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var lVal = lInts[lResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < rResult.state.size; i++) {
                                            var pos = rResult.state.selectedValuesPos[i];
                                            results[pos] = (lVal > rInts[pos]);
                                        }
                                    };
                                } else {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        for (var i = 0; i < result.state.size; i++) {
                                            var pos = result.state.getCurrSelectedValuesPos();
                                            results[pos] = (lInts[pos] > rInts[pos]);
                                        }
                                    };
                                }
                            case LESS_THAN_OR_EQUAL:
                                if (isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        results[0] =
                                            (lInts[lResult.state.getCurrSelectedValuesPos()] <=
                                                rInts[rResult.state.getCurrSelectedValuesPos()]);
                                    };
                                } else if (!isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var rVal = rInts[rResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < lResult.state.size; i++) {
                                            var pos = lResult.state.selectedValuesPos[i];
                                            results[pos] = (lInts[pos] <= rVal);
                                        }
                                    };
                                } else if (isLFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var lVal = lInts[lResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < rResult.state.size; i++) {
                                            var pos = rResult.state.selectedValuesPos[i];
                                            results[pos] = (lVal <= rInts[pos]);
                                        }
                                    };
                                } else {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        for (var i = 0; i < result.state.size; i++) {
                                            var pos = result.state.getCurrSelectedValuesPos();
                                            results[pos] = (lInts[pos] <= rInts[pos]);
                                        }
                                    };
                                }
                            case LESS_THAN:
                                if (isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        results[0] =
                                            (lInts[lResult.state.getCurrSelectedValuesPos()] <
                                                rInts[rResult.state.getCurrSelectedValuesPos()]);
                                    };
                                } else if (!isLFlat && isRFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var rVal = rInts[rResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < lResult.state.size; i++) {
                                            var pos = lResult.state.selectedValuesPos[i];
                                            results[pos] = (lInts[pos] < rVal);
                                        }
                                    };
                                } else if (isLFlat) {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        var lVal = lInts[lResult.state.getCurrSelectedValuesPos()];
                                        for (var i = 0; i < rResult.state.size; i++) {
                                            var pos = rResult.state.selectedValuesPos[i];
                                            results[pos] = (lVal < rInts[pos]);
                                        }
                                    };
                                } else {
                                    return () -> {
                                        lEvaluator.evaluate();
                                        rEvaluator.evaluate();
                                        for (var i = 0; i < result.state.size; i++) {
                                            var pos = result.state.getCurrSelectedValuesPos();
                                            results[pos] = (lInts[pos] < rInts[pos]);
                                        }
                                    };
                                }
                            default:
                                throw new UnsupportedOperationException("Comparison operator " +
                                    comparisonOperator.name() + " is not supported for ints.");
                        }
                    default:
                        throw new UnsupportedOperationException("Comparison operator " +
                            comparisonOperator.name() + " is not supported for Strings.");
                }
            case STRING:
                if (rightExpression.getDataType() != DataType.STRING) {
                    throw new UnsupportedOperationException("Comparison operator " +
                        comparisonOperator.name() + " between non strings is not supported.");
                }
                var lStrs = lResult.getStrings();
                var rStrs = rResult.getStrings();
                switch (comparisonOperator) {
                    case EQUALS:
                        if (isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                results[0] = lVal != null && lVal.equals(
                                    rStrs[rResult.state.getCurrSelectedValuesPos()]);
                            };
                        } else if (!isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var rVal = rStrs[rResult.state.getCurrSelectedValuesPos()];
                                var isRValNotNull = rVal != null;
                                for (var i = 0; i < lResult.state.size; i++) {
                                    var pos = lResult.state.selectedValuesPos[i];
                                    results[pos] = isRValNotNull &&
                                        lStrs[pos] != null && lStrs[pos].equals(rVal);
                                }
                            };
                        } else if (isLFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                var isLValNotNull = lVal != null;
                                for (var i = 0; i < rResult.state.size; i++) {
                                    var pos = rResult.state.selectedValuesPos[i];
                                    results[pos] = isLValNotNull && lVal.equals(rStrs[pos]);
                                }
                            };
                        } else {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                for (var i = 0; i < result.state.size; i++) {
                                    var pos = result.state.getCurrSelectedValuesPos();
                                    results[pos] = lStrs[pos] != null &&
                                        lStrs[pos].equals(rStrs[pos]);
                                }
                            };
                        }
                    case NOT_EQUALS:
                        if (isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                results[0] = lVal != null && !lVal.equals(
                                    rStrs[rResult.state.getCurrSelectedValuesPos()]);
                            };
                        } else if (!isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var rVal = rStrs[rResult.state.getCurrSelectedValuesPos()];
                                var isRValNotNull = rVal != null;
                                for (var i = 0; i < lResult.state.size; i++) {
                                    var pos = lResult.state.selectedValuesPos[i];
                                    results[pos] = isRValNotNull &&
                                        lStrs[pos] != null && !lStrs[pos].equals(rVal);
                                }
                            };
                        } else if (isLFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                var isLValNotNull = lVal != null;
                                for (var i = 0; i < rResult.state.size; i++) {
                                    var pos = rResult.state.selectedValuesPos[i];
                                    results[pos] = isLValNotNull && !lVal.equals(rStrs[pos]);
                                }
                            };
                        } else {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                for (var i = 0; i < result.state.size; i++) {
                                    var pos = result.state.getCurrSelectedValuesPos();
                                    results[pos] = lStrs[pos] != null &&
                                        !lStrs[pos].equals(rStrs[pos]);
                                }
                            };
                        }
                    case GREATER_THAN:
                        if (isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                results[0] = lVal != null && lVal.compareTo(
                                    rStrs[rResult.state.getCurrSelectedValuesPos()]) > 0;
                            };
                        } else if (!isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var rVal = rStrs[rResult.state.getCurrSelectedValuesPos()];
                                var isRValNotNull = rVal != null;
                                for (var i = 0; i < lResult.state.size; i++) {
                                    var pos = lResult.state.selectedValuesPos[i];
                                    results[pos] = isRValNotNull &&
                                        lStrs[pos] != null && lStrs[pos].compareTo(rVal) > 0;
                                }
                            };
                        } else if (isLFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                var isLValNotNull = lVal != null;
                                for (var i = 0; i < rResult.state.size; i++) {
                                    var pos = rResult.state.selectedValuesPos[i];
                                    results[pos] = isLValNotNull && lVal.compareTo(rStrs[pos]) > 0;
                                }
                            };
                        } else {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                for (var i = 0; i < result.state.size; i++) {
                                    var pos = result.state.getCurrSelectedValuesPos();
                                    results[pos] = lStrs[pos] != null &&
                                        lStrs[pos].compareTo(rStrs[pos]) > 0;
                                }
                            };
                        }
                    case GREATER_THAN_OR_EQUAL:
                        if (isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                results[0] = lVal != null && lVal.compareTo(
                                    rStrs[rResult.state.getCurrSelectedValuesPos()]) >= 0;
                            };
                        } else if (!isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var rVal = rStrs[rResult.state.getCurrSelectedValuesPos()];
                                var isRValNotNull = rVal != null;
                                for (var i = 0; i < lResult.state.size; i++) {
                                    var pos = lResult.state.selectedValuesPos[i];
                                    results[pos] = isRValNotNull &&
                                        lStrs[pos] != null && lStrs[pos].compareTo(rVal) >= 0;
                                }
                            };
                        } else if (isLFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                var isLValNotNull = lVal != null;
                                for (var i = 0; i < rResult.state.size; i++) {
                                    var pos = rResult.state.selectedValuesPos[i];
                                    results[pos] = isLValNotNull && lVal.compareTo(rStrs[pos]) >= 0;
                                }
                            };
                        } else {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                for (var i = 0; i < result.state.size; i++) {
                                    var pos = result.state.getCurrSelectedValuesPos();
                                    results[pos] = lStrs[pos] != null &&
                                        lStrs[pos].compareTo(rStrs[pos]) >= 0;
                                }
                            };
                        }
                    case LESS_THAN:
                        if (isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                results[0] = lVal != null && lVal.compareTo(
                                    rStrs[rResult.state.getCurrSelectedValuesPos()]) < 0;
                            };
                        } else if (!isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var rVal = rStrs[rResult.state.getCurrSelectedValuesPos()];
                                var isRValNotNull = rVal != null;
                                for (var i = 0; i < lResult.state.size; i++) {
                                    var pos = lResult.state.selectedValuesPos[i];
                                    results[pos] = isRValNotNull &&
                                        lStrs[pos] != null && lStrs[pos].compareTo(rVal) < 0;
                                }
                            };
                        } else if (isLFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                var isLValNotNull = lVal != null;
                                for (var i = 0; i < rResult.state.size; i++) {
                                    var pos = rResult.state.selectedValuesPos[i];
                                    results[pos] = isLValNotNull && lVal.compareTo(rStrs[pos]) < 0;
                                }
                            };
                        } else {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                for (var i = 0; i < result.state.size; i++) {
                                    var pos = result.state.getCurrSelectedValuesPos();
                                    results[pos] = lStrs[pos] != null &&
                                        lStrs[pos].compareTo(rStrs[pos]) < 0;
                                }
                            };
                        }
                    case LESS_THAN_OR_EQUAL:
                        if (isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                results[0] = lVal != null && lVal.compareTo(
                                    rStrs[rResult.state.getCurrSelectedValuesPos()]) <= 0;
                            };
                        } else if (!isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var rVal = rStrs[rResult.state.getCurrSelectedValuesPos()];
                                var isRValNotNull = rVal != null;
                                for (var i = 0; i < lResult.state.size; i++) {
                                    var pos = lResult.state.selectedValuesPos[i];
                                    results[pos] = isRValNotNull &&
                                        lStrs[pos] != null && lStrs[pos].compareTo(rVal) <= 0;
                                }
                            };
                        } else if (isLFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                var isLValNotNull = lVal != null;
                                for (var i = 0; i < rResult.state.size; i++) {
                                    var pos = rResult.state.selectedValuesPos[i];
                                    results[pos] = isLValNotNull && lVal.compareTo(rStrs[pos]) <= 0;
                                }
                            };
                        } else {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                for (var i = 0; i < result.state.size; i++) {
                                    var pos = result.state.getCurrSelectedValuesPos();
                                    results[pos] = lStrs[pos] != null &&
                                        lStrs[pos].compareTo(rStrs[pos]) <= 0;
                                }
                            };
                        }
                    case CONTAINS:
                        if (isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                results[0] = lVal != null && lVal.contains(
                                    rStrs[rResult.state.getCurrSelectedValuesPos()]);
                            };
                        } else if (!isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var rVal = rStrs[rResult.state.getCurrSelectedValuesPos()];
                                var isRValNotNull = rVal != null;
                                for (var i = 0; i < lResult.state.size; i++) {
                                    var pos = lResult.state.selectedValuesPos[i];
                                    results[pos] = isRValNotNull &&
                                        lStrs[pos] != null && lStrs[pos].contains(rVal);
                                }
                            };
                        } else if (isLFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                var isLValNotNull = lVal != null;
                                for (var i = 0; i < rResult.state.size; i++) {
                                    var pos = rResult.state.selectedValuesPos[i];
                                    results[pos] = isLValNotNull && lVal.contains(rStrs[pos]);
                                }
                            };
                        } else {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                for (var i = 0; i < result.state.size; i++) {
                                    var pos = result.state.getCurrSelectedValuesPos();
                                    results[pos] = lStrs[pos] != null &&
                                        lStrs[pos].contains(rStrs[pos]);
                                }
                            };
                        }
                    case STARTS_WITH:
                        if (isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                results[0] = lVal != null && lVal.startsWith(
                                    rStrs[rResult.state.getCurrSelectedValuesPos()]);
                            };
                        } else if (!isLFlat && isRFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var rVal = rStrs[rResult.state.getCurrSelectedValuesPos()];
                                var isRValNotNull = rVal != null;
                                for (var i = 0; i < lResult.state.size; i++) {
                                    var pos = lResult.state.selectedValuesPos[i];
                                    results[pos] = isRValNotNull &&
                                        lStrs[pos] != null && lStrs[pos].startsWith(rVal);
                                }
                            };
                        } else if (isLFlat) {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                var lVal = lStrs[lResult.state.getCurrSelectedValuesPos()];
                                var isLValNotNull = lVal != null;
                                for (var i = 0; i < rResult.state.size; i++) {
                                    var pos = rResult.state.selectedValuesPos[i];
                                    results[pos] = isLValNotNull && lVal.startsWith(rStrs[pos]);
                                }
                            };
                        } else {
                            return () -> {
                                lEvaluator.evaluate();
                                rEvaluator.evaluate();
                                for (var i = 0; i < result.state.size; i++) {
                                    var pos = result.state.getCurrSelectedValuesPos();
                                    results[pos] = lStrs[pos] != null &&
                                        lStrs[pos].startsWith(rStrs[pos]);
                                }
                            };
                        }
                    default:
                        throw new UnsupportedOperationException("Comparison operator " +
                            comparisonOperator.name() + " is not supported for ints.");
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
            if (literalTerm instanceof IntLiteral) {
                comparisonOperator = ComparisonOperator.LESS_THAN_OR_EQUAL;
                var longLiteral = (IntLiteral) literalTerm;
                longLiteral.setNewLiteralValue(longLiteral.getIntLiteral() - 1);
            } else if (literalTerm instanceof DoubleLiteral) {
                comparisonOperator = ComparisonOperator.LESS_THAN_OR_EQUAL;
                var doubleLiteral = (DoubleLiteral) literalTerm;
                doubleLiteral.setNewLiteralValue(doubleLiteral.getDoubleLiteral() - Double.MIN_VALUE);
            }
        } else if (comparisonOperator == ComparisonOperator.GREATER_THAN) {
            if (literalTerm instanceof IntLiteral) {
                comparisonOperator = ComparisonOperator.GREATER_THAN_OR_EQUAL;
                var longLiteral = (IntLiteral) literalTerm;
                longLiteral.setNewLiteralValue(longLiteral.getIntLiteral() + 1);
            } else if (literalTerm instanceof DoubleLiteral) {
                comparisonOperator = ComparisonOperator.GREATER_THAN_OR_EQUAL;
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
        hash = 31 * hash + comparisonOperator.hashCode();
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
