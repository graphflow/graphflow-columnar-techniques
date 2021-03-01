package ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;

public abstract class Filter extends AbstractExpressionEvaluatingOperator {

    Filter(Expression predicate, Schema inSchema) {
        super(predicate);
        this.outputTuple = new Tuple(inSchema.copy());
    }

    @Override
    public void initFurther(Graph graph) {
        setInputTupleCopyOverToOutputTupleAndExtendBy(0);
        evaluator = expression.getEvaluator(inputTuple, graph);
    }

    public static Filter make(Expression predicate, Schema inSchema) {
        return new FlatFilter(predicate, inSchema);
    }
}
