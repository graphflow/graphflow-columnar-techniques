package ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator;

import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.Value;

public interface ExpressionEvaluator {

    Value evaluate(Tuple tupleToEvaluator);
}
