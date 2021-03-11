package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public class AliasExpression extends UnaryOperatorExpression {

    public AliasExpression(Expression expression, String alias) {
        super(alias, expression);
        setDataType(expression.getDataType());
    }

    @Override
    public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
        return expression.getEvaluator(dataChunks);
    }

    @Override
    public String getPrintableExpression() {
        return expression.getPrintableExpression() + " AS " + getVariableName();
    }

    @Override
    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {
        expression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        if (DataType.NODE == expression.getDataType() ||
            DataType.RELATIONSHIP == expression.getDataType()) {
            throw new MalformedQueryException("[AliasExpression] Query Vertex or Edge cannot be " +
                "aliased.");
        }
        this.setDataType(expression.getDataType());
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        hash = 31*hash + "Alias".hashCode();
        return hash;
    }
}
