package ca.waterloo.dsg.graphflow.plan.operator.orderby;

import ca.waterloo.dsg.graphflow.parser.query.OrderByConstraint;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Table;
import ca.waterloo.dsg.graphflow.tuple.Tuple;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class OrderBy extends Operator {

    private List<OrderByConstraint> orderByConstraints;
    private Table outputTable;

    public OrderBy(List<OrderByConstraint> orderByConstraints, Schema inSchema) {
        this.outputTuple = new Tuple(inSchema.copy());
        this.orderByConstraints = orderByConstraints;
    }

    @Override
    public void initFurther(Graph graph) {
        setInputTupleCopyOverToOutputTupleAndExtendBy(0);
        this.outputTable = new Table(inputTuple);
    }

    @Override
    public void processNewTuple() {
        outputTable.add(inputTuple);
    }

    @Override
    public void notifyAllDone() {
        int[] orderByIndices = getOrderByIndices();
        Collections.sort(outputTable.getTuples(), new TupleComparator(orderByIndices, orderByConstraints));
        for (var i = 0; i < outputTable.getTuples().size(); ++i) {
            for (var j = 0; j < outputTuple.numValues(); ++j) {
                outputTuple.set(j, outputTable.getTuples().get(i).get(j));
            }
            numOutTuples++;
            next.processNewTuple();
        }
        next.notifyAllDone();
    }

    private int[] getOrderByIndices () {
        int[] orderByIndices = new int[orderByConstraints.size()];
        for (var i = 0; i < orderByConstraints.size(); ++i) {
            orderByIndices[i] =
                inputTuple.getIdx(orderByConstraints.get(i).getExpression().getVariableName());
        }
        return orderByIndices;
    }

    private static class TupleComparator implements Comparator<Tuple> {
        int[] orderByColIndices;
        List<OrderByConstraint> orderByConstraints;

        public TupleComparator(int[] orderByColIndices, List<OrderByConstraint> orderByConstraints) {
            this.orderByColIndices = orderByColIndices;
            this.orderByConstraints = orderByConstraints;
        }

        @Override
        public int compare(Tuple a, Tuple b) {
            for (var i = 0; i < orderByColIndices.length; ++i) {
                var orderByColIdx = orderByColIndices[i];
                int compareVal = a.get(orderByColIdx).compareTo(b.get(orderByColIdx));
                if (0 != compareVal) {
                    switch (orderByConstraints.get(i).getOrderType()) {
                        case DESCENDING:
                            return -compareVal;
                        case ASCENDING:
                            return compareVal;
                        default:
                            throw new IllegalArgumentException("This should never happen. Order by "
                                + orderByConstraints.get(i).getOrderType() + " is not supported.");
                    }
                }
            }
            return 0;
        }
    }
}
