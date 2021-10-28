package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.parser.ParserMethodReturnValue;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.tuple.value.flat.BoolVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.DoubleVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.StringVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract class Expression implements ParserMethodReturnValue, Serializable {

    @Getter @Setter
    String variableName;

    @Getter @Setter
    public DataType dataType;

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

    public ExpressionEvaluator getEvaluator(Tuple sampleTuple, Graph graph) {
        int operandIdx = sampleTuple.getIdx(variableName);
        switch (dataType) {
            case NODE:
                Value valToReuse = new IntVal(DataType.NULL_INTEGER);
                return (Tuple tupleToEvaluator) -> {
                    valToReuse.setInt((int) tupleToEvaluator.get(operandIdx).getNodeOffset());
                    return valToReuse;
                };
            case BOOLEAN:
                valToReuse = new BoolVal(false);
                return (Tuple tupleToEvaluator) -> {
                    valToReuse.setBool(tupleToEvaluator.get(operandIdx).getBool());
                    return valToReuse;
                };
            case DOUBLE:
                valToReuse = new DoubleVal(DataType.NULL_DOUBLE);
                return (Tuple tupleToEvaluator) -> {
                    valToReuse.setDouble(tupleToEvaluator.get(operandIdx).getDouble());
                    return valToReuse;
                };
            case INT:
                valToReuse = new IntVal(DataType.NULL_INTEGER);
                return (Tuple tupleToEvaluator) -> {
                    valToReuse.setInt(tupleToEvaluator.get(operandIdx).getInt());
                    return valToReuse;
                };
            case STRING:
                valToReuse = new StringVal(null);
                return (Tuple tupleToEvaluator) -> {
                    valToReuse.setString(tupleToEvaluator.get(operandIdx).getString());
                    return valToReuse;
                };
            default:
                throw new UnsupportedOperationException("Cannot read property with type: "
                    + dataType.name() + " in SimpleVariable.getEvaluator().");
        }
    }

    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {}

    public abstract Set<String> getDependentVariableNames();

    public abstract Set<String> getDependentExpressionVariableNames();

    /**
     * Warning: This method can return two PropertyVariable that actually refer to the same
     * variable, but were somehow created or cloned. So expect duplicates when calling.
     */
    public abstract Set<PropertyVariable> getDependentPropertyVariables();

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

    public String getSingleDependentVariableOrNull() {
        var dependentVars = getDependentVariableNames();
        if (dependentVars.isEmpty()) {
            return null;
        } else if (1 == dependentVars.size()) {
            return dependentVars.iterator().next();
        } else {
            return null;
        }
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