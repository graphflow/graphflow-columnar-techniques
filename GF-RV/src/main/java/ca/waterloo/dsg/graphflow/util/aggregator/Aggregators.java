package ca.waterloo.dsg.graphflow.util.aggregator;

import ca.waterloo.dsg.graphflow.tuple.Tuple;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class Aggregators {

    public static abstract class FirstValueReturningAggregator implements Aggregator, Serializable {
        @Setter
        int variableToAggregateTupleIdx;

        public void init(int variableToAggregateTupleIdx) {
            this.variableToAggregateTupleIdx = variableToAggregateTupleIdx;
        }
    }

    public static abstract class IntFirstValueReturningAggregator extends FirstValueReturningAggregator {
        public void assignFirstValue(Tuple tuple, AggregateValue aggregateValue) {
            ((IntAggregateValue) aggregateValue).setValue(tuple.get(variableToAggregateTupleIdx).getInt());
        }
    }

    public static abstract class DoubleFirstValueReturningAggregator extends FirstValueReturningAggregator {
        public void assignFirstValue(Tuple tuple, AggregateValue aggregateValue) {
            ((DoubleAggregateValue) aggregateValue).setValue(tuple.get(variableToAggregateTupleIdx).getDouble());
        }
    }

    public static class IntMaxAggregator extends IntFirstValueReturningAggregator {
        public void aggregate(Tuple tuple, AggregateValue aggregateValue) {
            ((IntAggregateValue) aggregateValue).setValue(
                Math.max(((IntAggregateValue) aggregateValue).getValue(),
                    tuple.get(variableToAggregateTupleIdx).getInt()));
        }
    }

    public static class DoubleMaxAggregator extends DoubleFirstValueReturningAggregator {
        public void aggregate(Tuple tuple, AggregateValue aggregateValue) {
            ((DoubleAggregateValue) aggregateValue).setValue(
                Math.max(((DoubleAggregateValue) aggregateValue).getValue(),
                    tuple.get(variableToAggregateTupleIdx).getDouble()));
        }
    }

    public static class IntMinAggregator extends IntFirstValueReturningAggregator {
        public void aggregate(Tuple tuple, AggregateValue aggregateValue) {
            ((IntAggregateValue) aggregateValue).setValue(
                Math.min(((IntAggregateValue) aggregateValue).getValue(),
                    tuple.get(variableToAggregateTupleIdx).getInt()));
        }
    }

    public static class DoubleMinAggregator extends DoubleFirstValueReturningAggregator {
        public void aggregate(Tuple tuple, AggregateValue aggregateValue) {
            ((DoubleAggregateValue) aggregateValue).setValue(
                Math.min(((DoubleAggregateValue) aggregateValue).getValue(),
                    tuple.get(variableToAggregateTupleIdx).getDouble()));
        }
    }

    public static class IntSumAggregator extends IntFirstValueReturningAggregator {
        public void aggregate(Tuple tuple, AggregateValue aggregateValue) {
            ((IntAggregateValue) aggregateValue).setValue(
                ((IntAggregateValue) aggregateValue).getValue() +
                    tuple.get(variableToAggregateTupleIdx).getInt());
        }
    }

    public static class DoubleSumAggregator extends DoubleFirstValueReturningAggregator {
        public void aggregate(Tuple tuple, AggregateValue aggregateValue) {
            ((DoubleAggregateValue) aggregateValue).setValue(
                ((DoubleAggregateValue) aggregateValue).getValue() +
                    tuple.get(variableToAggregateTupleIdx).getDouble());
        }
    }

    public static class CountStarAggregator implements Aggregator, Serializable {
        public void init(int variableToAggregateTupleIdx) { }

        public void assignFirstValue(Tuple tuple, AggregateValue aggregateValue) {
            ((IntAggregateValue) aggregateValue).setValue(1);
        }

        public void aggregate(Tuple tuple, AggregateValue aggregateValue) {
            ((IntAggregateValue) aggregateValue).setValue(
                ((IntAggregateValue) aggregateValue).getValue() + 1);
        }
    }

    public static class AggregateValue { }

    public static class IntAggregateValue extends AggregateValue {
        @Getter @Setter
        private int value = Integer.MIN_VALUE;
    }

    public static class DoubleAggregateValue extends AggregateValue{
        @Getter @Setter
        private double value = Double.MIN_VALUE;
    }
}
