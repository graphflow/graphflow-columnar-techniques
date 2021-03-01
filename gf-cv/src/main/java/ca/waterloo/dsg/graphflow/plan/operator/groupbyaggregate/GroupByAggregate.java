package ca.waterloo.dsg.graphflow.plan.operator.groupbyaggregate;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.query.expressions.FunctionInvocation;
import ca.waterloo.dsg.graphflow.parser.query.expressions.SimpleVariable;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.AggregationFunction;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.RelVal;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregator;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregators.AggregateValue;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregators.CountStarAggregator;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregators.DoubleAggregateValue;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregators.DoubleMaxAggregator;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregators.DoubleMinAggregator;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregators.DoubleSumAggregator;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregators.IntAggregateValue;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregators.IntMaxAggregator;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregators.IntMinAggregator;
import ca.waterloo.dsg.graphflow.util.aggregator.Aggregators.IntSumAggregator;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class GroupByAggregate extends Operator {
    static final Value DEFAULT_GROUPBY_KEY = new IntVal("", Integer.MIN_VALUE);
    public static final String COUNT_STAR_VAR_NAME = "COUNT STAR";
    @Getter List<String> inputTupleVariablesToAggregate;
    @Getter List<String> outputTupleAggregatedVariables;
    @Getter List<String> inputTupleGroupByVariableNames;
    List<Aggregator> aggregators;
    HashMap<List<Value>, List<AggregateValue>> aggregationMap;
    private List<Integer> inputTupleGroupByVariablesIdxs;
    private List<Integer> outputTupleGroupByVariablesIdxs;
    private List<Integer> inputTupleVariablesToAggregateIdxs;
    private List<Integer> outputTupleAggregatedVariablesTupleIdxs;
    private List<Value> groupByKey;
    List<AggregateValue> defaultAggregatedValues;

    public GroupByAggregate(List<String> inputTupleGroupByVariableNames,
        List<String> inputTupleVariablesToAggregate, List<String> outputTupleAggregatedVariables,
        List<FunctionInvocation> functionInvocations, Schema inSchema) {
        this.inputTupleVariablesToAggregate = inputTupleVariablesToAggregate;
        this.outputTupleAggregatedVariables = outputTupleAggregatedVariables;
        this.inputTupleGroupByVariableNames = inputTupleGroupByVariableNames;
        this.aggregationMap = new HashMap<>();
        inputTupleGroupByVariablesIdxs = new ArrayList<>();
        outputTupleGroupByVariablesIdxs = new ArrayList<>();
        inputTupleVariablesToAggregateIdxs = new ArrayList<>();
        outputTupleAggregatedVariablesTupleIdxs = new ArrayList<>();
        aggregators = new ArrayList<>();
        setOutputTupleAndAggregator(inputTupleVariablesToAggregate,
            outputTupleAggregatedVariables, functionInvocations, inSchema);
        inputTupleGroupByVariableNames.forEach(inputTupleGroupByVariableName -> {
            getOutSchema().add(inputTupleGroupByVariableName,
                inSchema.getExpression(inputTupleGroupByVariableName));
        });
        groupByKey = new ArrayList<>();
    }

    private void setOutputTupleAndAggregator(List<String> inputTupleVariablesToAggregate,
        List<String> outputTupleAggregatedVariables, List<FunctionInvocation> functionInvocations, Schema inputSchema) {
        var outSchema = new Schema();
        for (int i = 0; i < inputTupleVariablesToAggregate.size() ; ++i) {
            if (AggregationFunction.COUNT_STAR == functionInvocations.get(i).getFunction()) {
                this.aggregators.add(new CountStarAggregator());
                outSchema.add(outputTupleAggregatedVariables.get(i),
                    new SimpleVariable(outputTupleAggregatedVariables.get(i), DataType.INT));
            } else {
                var variableToAggregateExpr =
                    inputSchema.getExpression(inputTupleVariablesToAggregate.get(i));
                outSchema.add(outputTupleAggregatedVariables.get(i),
                    inputSchema.getExpression(inputTupleVariablesToAggregate.get(i)));
                switch (functionInvocations.get(i).getFunction()) {
                    case MIN:
                        if (DataType.DOUBLE == variableToAggregateExpr.getDataType()) {
                            this.aggregators.add(new DoubleMinAggregator());
                        } else if (DataType.INT == variableToAggregateExpr.getDataType()) {
                            this.aggregators.add(new IntMinAggregator());
                        } else {
                            throw new MalformedQueryException("Cannot find max variables other " +
                                "than INT, LONG or DOUBLE.");
                        }
                        break;
                    case MAX:
                        if (DataType.DOUBLE == variableToAggregateExpr.getDataType()) {
                            this.aggregators.add(new DoubleMaxAggregator());
                        } else if (DataType.INT == variableToAggregateExpr.getDataType()) {
                            this.aggregators.add(new IntMaxAggregator());
                        } else {
                            throw new MalformedQueryException("Cannot find max variables other " +
                                "than INT, LONG or DOUBLE.");
                        }
                        break;
                    case SUM:
                        if (DataType.INT == variableToAggregateExpr.getDataType()) {
                            this.aggregators.add(new IntSumAggregator());
                        } else if (DataType.DOUBLE == variableToAggregateExpr.getDataType()) {
                            this.aggregators.add(new DoubleSumAggregator());
                        } else {
                            throw new MalformedQueryException("Cannot sum variables other than " +
                                "INT, LONG or DOUBLE.");
                        }
                        break;
                    default:
                        throw new MalformedQueryException("This should not happen. Aggregation " +
                            "function: " + functionInvocations.get(i).getFunction() + " is not " +
                            "known.");
                }
            }

        }
        this.outputTuple = new Tuple(outSchema);
    }

    @Override
    public void initFurther(Graph graph) {
        inputTupleGroupByVariablesIdxs.clear();
        outputTupleGroupByVariablesIdxs.clear();
        inputTupleVariablesToAggregateIdxs.clear();
        outputTupleAggregatedVariablesTupleIdxs.clear();
        this.inputTuple = prev.getOutputTuple();
        // Warning: This is necessary because in EndToEndTests some operators get
        // initialized multiple times. This is because we do shallow copy of operators
        // during plan enumeration and reuse operators across plans.
        this.outputTuple = new Tuple(outputTuple.getSchema());
        for (int i = 0; i < inputTupleGroupByVariableNames.size(); ++i) {
            this.inputTupleGroupByVariablesIdxs.add(inputTuple.getIdx(inputTupleGroupByVariableNames.get(i)));
            outputTuple.append(ValueFactory.getFlatValueForDataType(inputTupleGroupByVariableNames.get(i),
                inputTuple.getSchema().getExpression(inputTupleGroupByVariableNames.get(i)).getDataType()),
                inputTuple.getSchema().getExpression(inputTupleGroupByVariableNames.get(i)));
            this.outputTupleGroupByVariablesIdxs.add(outputTuple.numValues() - 1);
        }
        for (int i = 0; i < inputTupleGroupByVariableNames.size(); ++i) {
            var varName = inputTupleGroupByVariableNames.get(i);
            var dataType = inputTuple.get(this.inputTupleGroupByVariablesIdxs.get(i)).getDataType();
            if (DataType.RELATIONSHIP == dataType) {
                ((RelVal) outputTuple.get(i)).setNodeVals(outputTuple, (RelVal) inputTuple
                    .get(inputTuple.getIdx(varName)));
            }
        }
        for (int i = 0; i < inputTupleVariablesToAggregate.size(); ++i) {
            this.inputTupleVariablesToAggregateIdxs.add(!COUNT_STAR_VAR_NAME.equals(inputTupleVariablesToAggregate.get(i)) ?
                inputTuple.getIdx(inputTupleVariablesToAggregate.get(i)) : Integer.MIN_VALUE);
            var expr = getOutSchema().getExpression(outputTupleAggregatedVariables.get(i));
            outputTuple.append(ValueFactory.getFlatValueForDataType(outputTupleAggregatedVariables.get(i),
                expr.getDataType()), expr);
            this.outputTupleAggregatedVariablesTupleIdxs.add(outputTuple.numValues() - 1);
            this.aggregators.get(i).init(inputTupleVariablesToAggregateIdxs.get(i));
        }
        this.aggregationMap.clear();
        this.groupByKey.clear();
        if (inputTupleGroupByVariableNames.isEmpty()) {
            groupByKey.add(DEFAULT_GROUPBY_KEY);
        }
        this.defaultAggregatedValues = null;
    }

    void groupByAggregateFlatTuple(Tuple tuple) {
        List<AggregateValue> aggregateValues = defaultAggregatedValues;
        if (!inputTupleGroupByVariableNames.isEmpty()) {
             groupByKey.clear();
             for (int i = 0; i < inputTupleGroupByVariableNames.size(); ++i) {
                 var sampleVal = tuple.get(inputTupleGroupByVariablesIdxs.get(i));
                 groupByKey.add(sampleVal);
             }
            aggregateValues = aggregationMap.get(groupByKey);
        }

        if (null == aggregateValues) {
            aggregateValues = new ArrayList<>();
            for (int i = 0; i < aggregators.size(); ++i) {
                AggregateValue aValue;
                if (inputTupleVariablesToAggregateIdxs.get(i) < 0) { // count(*) case
                    aValue = new IntAggregateValue();
                } else {
                    var dataType = tuple.get(inputTupleVariablesToAggregateIdxs.get(i)).getDataType();
                    if (DataType.INT == dataType) {
                        aValue = new IntAggregateValue();
                    } else if (DataType.DOUBLE == dataType) {
                        aValue = new DoubleAggregateValue();
                    } else {
                        throw new MalformedQueryException("Cannot aggregate variables other than INT, LONG " +
                            "or DOUBLE.");
                    }
                }
                aggregators.get(i).assignFirstValue(tuple, aValue);
                aggregateValues.add(aValue);
                if (inputTupleGroupByVariableNames.isEmpty()) {
                    defaultAggregatedValues = aggregateValues;
                }
            }
            var newGroupByKey = new ArrayList<Value>();
            for (var value : groupByKey) {
                newGroupByKey.add(value.copy());
            }
            for (var i = 0; i < groupByKey.size(); i++) {
                if (DataType.RELATIONSHIP == groupByKey.get(i).getDataType()) {
                    var relVal = (RelVal) groupByKey.get(i);
                    newGroupByKey.get(i).setRelSrcNodeVal((NodeVal) newGroupByKey.stream().filter(
                        value -> value.getVariableName().equals(relVal.getRelSrcNodeVal()
                            .getVariableName())).findAny().get());
                    newGroupByKey.get(i).setRelDstNodeVal((NodeVal) newGroupByKey.stream().filter(
                        value -> value.getVariableName().equals(relVal.getRelDstNodeVal()
                            .getVariableName())).findAny().get());
                }
            }
            aggregationMap.put(newGroupByKey, aggregateValues);
        } else {
            for (int i = 0; i < aggregators.size(); ++i) {
                aggregators.get(i).aggregate(tuple, aggregateValues.get(i));
            }
        }
    }

    public void notifyAllDone() {
        if (inputTupleGroupByVariableNames.isEmpty()) {
            List<Value> defaultKey = new ArrayList<>();
            defaultKey.add(DEFAULT_GROUPBY_KEY);
            var aggregateValues = aggregationMap.get(defaultKey);
            if (null != aggregateValues) {
                setOutputTupleValue(aggregateValues);
            }
        } else {
            for (var keyValue : aggregationMap.entrySet()) {
                for (int i = 0; i < outputTupleGroupByVariablesIdxs.size(); ++i) {
                    var outputTupleGroupByVariableValue = outputTuple.get(outputTupleGroupByVariablesIdxs.get(i));
                    outputTupleGroupByVariableValue.set(keyValue.getKey().get(i));
                }
                setOutputTupleValue(keyValue.getValue());
            }
        }
        next.notifyAllDone();
    }

    private void setOutputTupleValue(List<AggregateValue> aggregateValues) {
        for (int i = 0; i < outputTupleAggregatedVariablesTupleIdxs.size(); ++i) {
            var usingTuple = outputTuple.get(outputTupleAggregatedVariablesTupleIdxs.get(i));
            switch (usingTuple.getDataType()) {
                case INT:
                    usingTuple.setInt(((IntAggregateValue) aggregateValues.get(i)).getValue());
                    break;
                case DOUBLE:
                    usingTuple.setDouble(((DoubleAggregateValue) aggregateValues.get(i)).getValue());
                    break;
            }
        }
        next.processNewTuple();
    }

    public static GroupByAggregate make(List<String> inputTupleGroupByVariableNames,
        List<String> inputTupleVariablesToAggregate, List<String> outputTupleAggregatedVariables,
        List<FunctionInvocation> functionInvocations, Schema inSchema) {
        return new FlatGroupByAggregate(inputTupleGroupByVariableNames,
            inputTupleVariablesToAggregate, outputTupleAggregatedVariables,
            functionInvocations, inSchema);
    }
}
