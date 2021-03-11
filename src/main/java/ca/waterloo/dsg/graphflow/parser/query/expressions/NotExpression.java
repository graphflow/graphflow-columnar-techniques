package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorState;
import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public class NotExpression extends UnaryOperatorExpression {

    public NotExpression(Expression expression) {
        super("", expression);
        setVariableName(getPrintableExpression());
    }

    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {
        expression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        setDataType(DataType.BOOLEAN);
    }

    @Override
    public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
        var exprEvaluator = expression.getEvaluator(dataChunks);
        var exprResult = expression.result.getBooleans();
        var isFlat = ExpressionUtils.isExpressionOutputFlat(expression, dataChunks);

        var capacity = isFlat ? 1 : Vector.DEFAULT_VECTOR_SIZE;
        result =  Vector.make(DataType.BOOLEAN, capacity);
        result.state = isFlat ? VectorState.getFlatVectorState() : expression.result.state;
        var results = result.getBooleans();
        if (isFlat) {
            return () -> {
                exprEvaluator.evaluate();
                results[0] = !exprResult[result.state.getCurrSelectedValuesPos()];
            };
        } else {
            return () -> {
                exprEvaluator.evaluate();
                for (var i = 0; i < result.state.size; i++) {
                    var pos = result.state.selectedValuesPos[i];
                    results[pos] = !exprResult[pos];
                }
            };
        }
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        hash = 31*hash + "NOT".hashCode();
        return hash;
    }
}
