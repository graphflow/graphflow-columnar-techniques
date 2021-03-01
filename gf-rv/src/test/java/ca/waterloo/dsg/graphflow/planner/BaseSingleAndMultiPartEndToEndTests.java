package ca.waterloo.dsg.graphflow.planner;

import ca.waterloo.dsg.graphflow.tuple.Table;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;

public abstract class BaseSingleAndMultiPartEndToEndTests extends AbstractEndToEndTests {

    protected void testCountStar(String queryStr) {
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            Assertions.assertEquals(1, table.getTuples().size());
            Assertions.assertEquals(60, table.getTuples().iterator().next().get(0).getInt());
        });
    }

    protected void testMinDate(String queryStr) {
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.age",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("MIN(e1.date)",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{35, 1234567890});
            tupleIntValues.add(new int[]{30, 1234567890});
            tupleIntValues.add(new int[]{45, 1234567890});
            tupleIntValues.add(new int[]{20, 1234567890});
            tupleIntValues.add(new int[]{25, 1234567897});
            tupleIntValues.add(new int[]{40, 1234567897});
            Assertions.assertEquals(tupleIntValues.size(), table.getTuples().size());
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }
}
