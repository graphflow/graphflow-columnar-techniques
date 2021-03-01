package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.parser.query.returnorwith.AggregationFunction;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

/**
 * Warning: We assume for now that function calls can only be made for queries with group-by
 * and aggregates, i.e., we assume all function calls are aggregation functions.
 */
public class FunctionInvocation extends Expression {
    @Getter
    private Expression expression;
    @Getter
    private AggregationFunction function;

    /**
     * Warning: Only to be used for CountStar aggregations.
     */
    private FunctionInvocation() {
        super("COUNT(*)", DataType.INT);
        this.expression= null;
        this.function = AggregationFunction.COUNT_STAR;
    }

    public FunctionInvocation(AggregationFunction function, Expression expression) {
        super(function.name() + "(" + expression.getVariableName() + ")",
            expression.getDataType());
        this.expression= expression;
        this.function = function;
    }

    @Override
    public Set<FunctionInvocation> getDependentFunctionInvocations() {
        var retVal = null != expression ? expression.getDependentFunctionInvocations() :
            new HashSet<FunctionInvocation>();
        retVal.add(this);
        return retVal;
    }

    @Override
    public boolean hasDependentFunctionInvocations() {
        return true;
    }

    @Override
    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {
        if (null != expression) {
            expression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
            this.dataType = expression.getDataType();
        }
    }

    @Override
    public Set<String> getDependentVariableNames() {
        return null != expression ? expression.getDependentVariableNames() : new HashSet<>();
    }

    @Override
    public Set<String> getDependentExpressionVariableNames() {
        return null != expression ? expression.getDependentExpressionVariableNames() : new HashSet<>();
    }

    @Override
    public Set<PropertyVariable> getDependentPropertyVariables() {
        return null != expression ?  expression.getDependentPropertyVariables() : new HashSet<>();
    }

    public static FunctionInvocation newCountStar() {
        return new FunctionInvocation();
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        if (null != expression) {
            hash = 31*hash + expression.hashCode();
        }
        hash = hash*31 + function.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherVariable = (FunctionInvocation) o;
        return this.function == otherVariable.function &&
            ((null == this.expression && null == otherVariable.expression) ||
                (null != this.expression && null != otherVariable.expression &&
                    this.expression.equals(otherVariable.expression)));
    }
}
