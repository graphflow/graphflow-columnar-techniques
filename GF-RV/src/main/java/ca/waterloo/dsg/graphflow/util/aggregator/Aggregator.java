package ca.waterloo.dsg.graphflow.util.aggregator;

import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregators.AggregateValue;

public interface Aggregator {

    void init(int variableToAggregateIdx);

    void aggregate(Tuple tuple, AggregateValue aggregateValue);

    void assignFirstValue(Tuple tuple, AggregateValue aggregateValue);
}
