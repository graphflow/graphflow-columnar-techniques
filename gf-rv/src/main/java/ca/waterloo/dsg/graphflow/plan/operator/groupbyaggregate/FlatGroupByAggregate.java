package ca.waterloo.dsg.graphflow.plan.operator.groupbyaggregate;

import ca.waterloo.dsg.graphflow.parser.query.expressions.FunctionInvocation;
import ca.waterloo.dsg.graphflow.tuple.Schema;

import java.util.List;

public class FlatGroupByAggregate extends GroupByAggregate {

    // Warning: inputTupleGroupByVariable is null when there is no group by key.
    // Warning: inputTupleVariableToAggregate is null when the aggregator is a CountStar aggregator.
    public FlatGroupByAggregate(List<String> inputTupleGroupByVariableNames,
        List<String> inputTupleVariablesToAggregate, List<String> outputTupleAggregatedVariables,
        List<FunctionInvocation> functionInvocations, Schema inSchema) {
        super(inputTupleGroupByVariableNames, inputTupleVariablesToAggregate,
            outputTupleAggregatedVariables, functionInvocations, inSchema);
        operatorName = "FlatGroupByAggregate with outSchema: " +
            getOutSchema().getVariableNamesAsString();
    }

    @Override
    public void processNewTuple() {
        groupByAggregateFlatTuple(inputTuple);
    }
}
