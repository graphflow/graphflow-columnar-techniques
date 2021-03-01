package ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.tuple.Schema;

public class FlatFilter extends Filter {

    public FlatFilter(Expression predicate, Schema inSchema) {
        super(predicate, inSchema);
        var builder = new StringBuilder("FlatFilter ");
        builder.append(predicate.getPrintableExpression());
        operatorName = builder.toString();
    }

    @Override
    public void processNewTuple() {
        if (evaluator.evaluate(inputTuple).getBool()) {
            numOutTuples++;
            next.processNewTuple();
        }
    }
}
