package ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator.FlatExpressionEvaluatorWriter.FlatBooleanExpressionEvaluatorWriter;
import ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator.FlatExpressionEvaluatorWriter.FlatDoubleExpressionEvaluatorWriter;
import ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator.FlatExpressionEvaluatorWriter.FlatIntExpressionEvaluatorWriter;
import ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator.FlatExpressionEvaluatorWriter.FlatStringExpressionEvaluatorWriter;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;

public abstract class ExpressionEvaluatorWriter extends AbstractExpressionEvaluatingOperator {

    public ExpressionEvaluatorWriter(Expression expression, Schema inSchema) {
        super(expression);
        var outSchema = inSchema.copy();
        outSchema.add(expression.getVariableName(), expression);
        this.outputTuple = new Tuple(outSchema);
    }

    @Override
    public void initFurther(Graph graph) {
        setInputTupleCopyOverToOutputTupleAndExtendBy(1);
    }

    @Override
    public void processNewTuple() {
        evaluateAndWriteToOutputTuple();
        numOutTuples++;
        next.processNewTuple();
    }

    abstract void evaluateAndWriteToOutputTuple();

    public static ExpressionEvaluatorWriter constructExpressionEvaluatorOperator(
        Expression expression, Schema inSchema) {
        switch (expression.getDataType()) {
            case BOOLEAN:
                return new FlatBooleanExpressionEvaluatorWriter(expression, inSchema);
            case DOUBLE:
                return new FlatDoubleExpressionEvaluatorWriter(expression, inSchema);
            case INT:
                return new FlatIntExpressionEvaluatorWriter(expression, inSchema);
            case STRING:
                return new FlatStringExpressionEvaluatorWriter(expression, inSchema);
            default:
                throw new IllegalArgumentException("We do not support " +
                    "FlatExpressionEvaluatorOperator for data type: " +
                    expression.getDataType().name());
        }
    }
}
