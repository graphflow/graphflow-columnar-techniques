package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.flat.BoolVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public class NotExpression extends AbstractUnaryOperatorExpression {

    private Expression expression;

    public NotExpression(Expression expression) {
        super("", expression);
        this.expression = expression;
        setVariableName(getPrintableExpression());
    }

    public void verifyVariablesAndNormalize(Schema inputSchema,
        Schema matchGraphSchema, GraphCatalog catalog) {
        expression.verifyVariablesAndNormalize(inputSchema,
            matchGraphSchema, catalog);
        setDataType(DataType.BOOLEAN);
    }

    @Override
    public ExpressionEvaluator getEvaluator(Tuple sampleTuple, Graph graph) {
        var trueBoolVal = new BoolVal(true);
        var falseBoolVal = new BoolVal(false);
        var expressionEvaluator = expression.getEvaluator(sampleTuple, graph);
        return (Tuple tupleToEvaluator) -> {
            var boolVal = expressionEvaluator.evaluate(tupleToEvaluator);
            if (boolVal.getBool()) {
                return falseBoolVal;
            }
            return trueBoolVal;
        };
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        hash = 31*hash + "NOT".hashCode();
        return hash;
    }
}
