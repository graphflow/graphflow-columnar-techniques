package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.parser.query.expressions.BooleanConnectorExpression.ANDExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.BooleanConnectorExpression.ORExpression;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ExpressionUtils {

    public static boolean isExpressionOutputFlat(Expression expression, DataChunks dataChunks) {
        for (var variableName : expression.getDependentVariableNames()) {
            if (!dataChunks.isFlat(variableName)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isConjunctivePredicate(Expression expr) {
        return DataType.BOOLEAN == expr.getDataType() && !(expr instanceof ORExpression);
    }

    public static Expression projectOntoVars(Expression boolExpression, Schema schema) {
       return projectVars(boolExpression, schema, true);
    }

    private static Expression projectVars(Expression boolExpression,
        Schema schema, boolean isOnto) {
        if (DataType.BOOLEAN != boolExpression.getDataType()) {
            throw new UnsupportedOperationException("Cannot project an expression that is " +
                " of data type: " + boolExpression.getDataType());
        }
        if (!ExpressionUtils.isConjunctivePredicate(boolExpression)) {
            return projectEntireExpression(boolExpression, schema, isOnto);
        } else {
            return projectRecursive(boolExpression, schema, isOnto);
        }
    }

    private static Expression projectEntireExpression(Expression boolExpression, Schema schema,
        boolean isOnto) {
        var dependentVars = boolExpression.getDependentVariableNames();
        for (var dependentVar : dependentVars) {
            if (!schema.containsVarName(dependentVar)) {
                return isOnto ? null : boolExpression;
            }
        }
        return isOnto ? boolExpression : null;
    }

    private static Expression projectRecursive(Expression simpleConjunctiveExpr, Schema schema,
        boolean isOnto) {
        if (!(simpleConjunctiveExpr instanceof ANDExpression)) {
            var dependentVars = simpleConjunctiveExpr.getDependentVariableNames();
            for (var dependentVar : dependentVars) {
                if (!schema.containsVarName(dependentVar)) {
                    return isOnto ? null : simpleConjunctiveExpr;
                }
            }
            return isOnto ? simpleConjunctiveExpr : null;
        }
        var andExpression = (ANDExpression) simpleConjunctiveExpr;
        var leftProjectedExpr = projectRecursive(andExpression.leftExpression, schema, isOnto);
        var rightProjectedExpr = projectRecursive(andExpression.rightExpression, schema, isOnto);
        if (null != leftProjectedExpr && null != rightProjectedExpr) {
            var retVal = new ANDExpression(leftProjectedExpr, rightProjectedExpr);
            retVal.setDataType(simpleConjunctiveExpr.getDataType());
            return retVal;
        } else if (null == leftProjectedExpr && null == rightProjectedExpr) {
            return null;
        } else return Objects.requireNonNullElse(rightProjectedExpr, leftProjectedExpr);
    }

    public static List<ComparisonExpression> getComparisonExpressionsInConjunctiveParts(
        Expression simpleConjunctiveExpr) {
        if (simpleConjunctiveExpr instanceof ComparisonExpression) {
            List<ComparisonExpression> retVal = new ArrayList<>();
            retVal.add((ComparisonExpression) simpleConjunctiveExpr);
            return retVal;
        } else if (simpleConjunctiveExpr instanceof ANDExpression) {
            var andExpr = (ANDExpression) simpleConjunctiveExpr;
            var retVal = getComparisonExpressionsInConjunctiveParts(andExpr.leftExpression);
            retVal.addAll(getComparisonExpressionsInConjunctiveParts(andExpr.rightExpression));
            return retVal;
        } else {
           return new ArrayList<>();
        }
    }

    public static Expression removeComparisonExpression(Expression simpleConjunctiveExpr,
        ComparisonExpression comparisonExprToRemove) {
        if (simpleConjunctiveExpr == comparisonExprToRemove ||
            simpleConjunctiveExpr.equals(comparisonExprToRemove)) {
            return null;
        } else if (simpleConjunctiveExpr instanceof ANDExpression) {
            var andExpr = (ANDExpression) simpleConjunctiveExpr;
            var leftRemovedExpr = removeComparisonExpression(andExpr.leftExpression,
                comparisonExprToRemove);
            var rightRemovedExpr = removeComparisonExpression(andExpr.rightExpression,
                comparisonExprToRemove);
            if (null != leftRemovedExpr && null != rightRemovedExpr) {
                return new ANDExpression(leftRemovedExpr, rightRemovedExpr);
            } else if (null == leftRemovedExpr && null == rightRemovedExpr) {
                return null;
            } else if (null == leftRemovedExpr) {
                return rightRemovedExpr;
            } else {
                return leftRemovedExpr;
            }
        }
        return simpleConjunctiveExpr;
    }

    public static Set<String> getUnionedDependentVariables(Expression leftExpression,
        Expression rightExpression) {
        var retVal = leftExpression.getDependentVariableNames();
        retVal.addAll(rightExpression.getDependentVariableNames());
        return retVal;
    }

    public static Set<String> getUnionedDependentExpressionVariables(Expression leftExpression,
        Expression rightExpression) {
        var retVal = leftExpression.getDependentExpressionVariableNames();
        retVal.addAll(rightExpression.getDependentExpressionVariableNames());
        return retVal;
    }

    public static Set<PropertyVariable> getUnionedDependentPropertyVariables(
        Expression lExpr, Expression rExpr) {
        var retVal = lExpr.getDependentPropertyVars();
        retVal.addAll(rExpr.getDependentPropertyVars());
        return retVal;
    }

    public static Set<FunctionInvocation> getUnionedDependentFunctionInvocations(
        Expression leftExpression, Expression rightExpression) {
        var retVal = leftExpression.getDependentFunctionInvocations();
        retVal.addAll(rightExpression.getDependentFunctionInvocations());
        return retVal;
    }
}
