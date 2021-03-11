package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.parser.ParserMethodReturnValue;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static ca.waterloo.dsg.graphflow.parser.query.expressions.BooleanConnectorExpression.BooleanConnector.AND;

public abstract class Expression implements ParserMethodReturnValue, Serializable {

    @Getter protected Vector result;
    @Getter @Setter String variableName;
    @Getter @Setter public DataType dataType;

    public Expression(String variableName) {
        this(variableName, DataType.UNKNOWN);
    }

    public Expression(String variableName, DataType dataType) {
        this.variableName = variableName;
        this.dataType = dataType;
    }

    public String getPrintableExpression() {
        return variableName;
    }

    public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
        result = dataChunks.getValueVector(variableName);
        return () -> {};
    }

    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {}

    public List<Expression> splitPredicateClauses() {
        List<Expression> expressions = new ArrayList<>();
        if (this instanceof BooleanConnectorExpression &&
                AND == ((BooleanConnectorExpression) this).getBoolConnector()) {
            var expr = (BinaryOperatorExpression) this;
            expressions.addAll(expr.leftExpression.splitPredicateClauses());
            expressions.addAll(expr.rightExpression.splitPredicateClauses());
        } else {
            expressions.add(this);
        }
        return expressions;
    }

    public abstract Set<String> getDependentVariableNames();

    public abstract Set<String> getDependentExpressionVariableNames();

    /**
     * Warning: This method can return two PropertyVariable that actually refer to the same
     * variable, but were somehow created or cloned. So expect duplicates when calling.
     */
    public abstract Set<PropertyVariable> getDependentPropertyVars();

    /**
     * Warning: This method can return two FunctionInvocation that actually refer to the same
     * variable, but were somehow created or cloned. So expect duplicates when calling.
     */
    public Set<FunctionInvocation> getDependentFunctionInvocations() {
        return new HashSet<>();
    }

    public boolean hasDependentFunctionInvocations() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(variableName, dataType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (null == o) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        var other = (Expression) o;
        return this.variableName.equals(other.variableName) && this.dataType == other.dataType;
    }
}