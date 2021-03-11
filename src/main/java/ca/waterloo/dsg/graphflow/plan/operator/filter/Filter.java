package ca.waterloo.dsg.graphflow.plan.operator.filter;

import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;

public class Filter extends Operator {

    private final Expression predicates;

    private Vector result;
    private boolean[] results;
    private ExpressionEvaluator evaluator;

    private final boolean isFlat;

    public Filter(Expression predicates, boolean isFlat, Operator prev) {
        super(prev);
        this.predicates = predicates;
        this.isFlat = isFlat;
        operatorName = "Filter: " + predicates.getPrintableExpression();
    }

    @Override
    protected void initFurther(Graph graph) {
        evaluator = predicates.getEvaluator(dataChunks);
        result = predicates.getResult();
        results = result.getBooleans();
    }

    @Override
    public void processNewDataChunks() {
        evaluator.evaluate();
        if (isFlat) {
            if (results[result.state.getCurrSelectedValuesPos()]) {
                next.processNewDataChunks();
            }
        } else {
            var numSelectedValues = 0;
            for (var i = 0; i < result.state.size; i++) {
                var pos = result.state.selectedValuesPos[i];
                if (results[pos]) {
                    result.state.selectedValuesPos[numSelectedValues++] = pos;
                }
            }
            if (numSelectedValues == 0) return;
            result.state.size = numSelectedValues;
            next.processNewDataChunks();
        }
    }

    @Override
    public Filter copy() {
        return new Filter(predicates, isFlat, prev.copy());
    }
}
