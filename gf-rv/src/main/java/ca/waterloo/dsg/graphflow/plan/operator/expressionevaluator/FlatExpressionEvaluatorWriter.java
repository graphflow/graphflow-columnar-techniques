package ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;

public abstract class FlatExpressionEvaluatorWriter extends ExpressionEvaluatorWriter {
    protected Value valueToWriteTo;

    public FlatExpressionEvaluatorWriter(Expression expression, Schema inSchema) {
        super(expression, inSchema);
        var builder = new StringBuilder("FlatExpEvaluator ");
        builder.append(expression.getPrintableExpression());
        operatorName = builder.toString();
    }

    @Override
    public void initFurther(Graph graph) {
        super.initFurther(graph);
        valueToWriteTo = ValueFactory.getFlatValueForDataType(expression.getVariableName(),
            expression.getDataType());
        outputTuple.set(this.inputTuple.numValues(), valueToWriteTo);
        evaluator = expression.getEvaluator(inputTuple, graph);
    }

    public static class FlatBooleanExpressionEvaluatorWriter extends FlatExpressionEvaluatorWriter {

        public FlatBooleanExpressionEvaluatorWriter(Expression expression, Schema inSchema) {
            super(expression, inSchema);
        }

        @Override
        void evaluateAndWriteToOutputTuple() {
            valueToWriteTo.setBool(evaluator.evaluate(inputTuple).getBool());
        }
    }

    public static class FlatIntExpressionEvaluatorWriter extends FlatExpressionEvaluatorWriter {

        public FlatIntExpressionEvaluatorWriter(Expression expression, Schema inSchema) {
            super(expression, inSchema);
        }

        @Override
        void evaluateAndWriteToOutputTuple() {
            valueToWriteTo.setInt(evaluator.evaluate(inputTuple).getInt());
        }
    }

    public static class FlatDoubleExpressionEvaluatorWriter extends FlatExpressionEvaluatorWriter {

        public FlatDoubleExpressionEvaluatorWriter(Expression expression, Schema inSchema) {
            super(expression, inSchema);
        }

        @Override
        void evaluateAndWriteToOutputTuple() {
            valueToWriteTo.setDouble(evaluator.evaluate(inputTuple).getDouble());
        }
    }

    public static class FlatStringExpressionEvaluatorWriter extends FlatExpressionEvaluatorWriter {

        public FlatStringExpressionEvaluatorWriter(Expression expression, Schema inSchema) {
            super(expression, inSchema);
        }

        @Override
        void evaluateAndWriteToOutputTuple() {
            valueToWriteTo.setString(evaluator.evaluate(inputTuple).getString());
        }
    }
}
