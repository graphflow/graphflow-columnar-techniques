package ca.waterloo.dsg.graphflow.planner;

import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.tuple.value.flat.BoolVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.DoubleVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.StringVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;

public class GroupByAggregateTest extends AbstractEndToEndTests {

    @Test
    public void testGroupByVariableWithDifferentType() {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN a.gender, b.isStudent, b.gender, count(*)";
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.gender", DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.isStudent", DataType.BOOLEAN));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.gender", DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("COUNT(*)", DataType.INT));
        var tupleValues = new ArrayList<Value[]>();
        tupleValues.add(new Value[]{new IntVal("a.gender", 2),
            new BoolVal("b.isStudent", true), new IntVal("b.gender", 1),
            new IntVal("COUNT(*)", 2)});
        tupleValues.add(new Value[]{new IntVal("a.gender", 1),
            new BoolVal("b.isStudent", true), new IntVal("b.gender", 2),
            new IntVal("COUNT(*)", 3)});
        tupleValues.add(new Value[]{new IntVal("a.gender", 1),
            new BoolVal("b.isStudent", true), new IntVal("b.gender", 1),
            new IntVal("COUNT(*)", 1)});
        tupleValues.add(new Value[]{new IntVal("a.gender", 2),
            new BoolVal("b.isStudent", true), new IntVal("b.gender", 2),
            new IntVal("COUNT(*)", 1)});
        tupleValues.add(new Value[]{new IntVal("a.gender", 2),
            new BoolVal("b.isStudent", false), new IntVal("b.gender", 1),
            new IntVal("COUNT(*)", 2)});
        tupleValues.add(new Value[]{new IntVal("a.gender", 1),
            new BoolVal("b.isStudent", false), new IntVal("b.gender", 2),
            new IntVal("COUNT(*)", 3)});
        tupleValues.add(new Value[]{new IntVal("a.gender", 1),
            new BoolVal("b.isStudent", false), new IntVal("b.gender", 1),
            new IntVal("COUNT(*)", 1)});
        tupleValues.add(new Value[]{new IntVal("a.gender", 2),
            new BoolVal("b.isStudent", false), new IntVal("b.gender", 2),
            new IntVal("COUNT(*)", 1)});
        assertTest(queryStr, sampleTupleForTable, tupleValues);
    }

    @Test
    public void testAggregationVariableWithDifferentType() {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN e1.date, max(a.age), min(b.eyeSight), sum(a.gender)";
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("e1.date", DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("MAX(a.age)", DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("MIN(b.eyeSight)", DataType.DOUBLE));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("SUM(a.gender)", DataType.INT));
        var tupleValues = new ArrayList<Value[]>();
        tupleValues.add(new Value[]{new IntVal("e1.date", 1234567890),
            new IntVal("MAX(a.age)", 45), new DoubleVal("MIN(b.eyeSight)", 4.8),
            new IntVal("SUM(a.gender)", 8)});
        tupleValues.add(new Value[]{new IntVal("e1.date", 1234567892),
            new IntVal("MAX(a.age)", 45), new DoubleVal("MIN(b.eyeSight)", 4.8),
            new IntVal("SUM(a.gender)", 7)});
        tupleValues.add(new Value[]{new IntVal("e1.date", 1234567893),
            new IntVal("MAX(a.age)", 45), new DoubleVal("MIN(b.eyeSight)", 4.8),
            new IntVal("SUM(a.gender)", 3)});
        tupleValues.add(new Value[]{new IntVal("e1.date", 1234567897),
            new IntVal("MAX(a.age)", 20), new DoubleVal("MIN(b.eyeSight)", 4.5),
            new IntVal("SUM(a.gender)", 2)});
        assertTest(queryStr, sampleTupleForTable, tupleValues);
    }

    @Test
    public void testRelAsGroupBy() {
        var queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) RETURN a, e1, b, sum(a.age)";
        var plans = getAllUnfactorizedPlans(queryStr);
        plans.forEach(plan -> {
            plan.setStoreTuples(true);
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            Assertions.assertEquals(3 /*expected number of groups in table*/,
                table.getTuples().size());
            /* tests all relVal values to be unique*/
            testUniqueValues(table.getTuples().stream().map(tuple -> tuple.get(0)).collect(
                Collectors.toList()));
            /* tests all sum(a.age) values are as expected */
            var expectedSumVals = new HashSet<>(Arrays.asList(45, 20 /*2 groups have sum = 20*/));
            table.getTuples().forEach(tuple -> {
                var intVal = tuple.get(3);
                Assertions.assertTrue(expectedSumVals.contains(intVal.getInt()));
            });
        });
    }

    @Test
    public void testNoGroupByVariable() {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN max(a.age), min(a.age), sum(b.eyeSight), count(*)";
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("MAX(a.age)", DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("MIN(a.age)", DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("SUM(b.eyeSight)", DataType.DOUBLE));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("COUNT(*)", DataType.INT));
        var tupleValues = new ArrayList<Value[]>();
        tupleValues.add(new Value[]{new IntVal("MAX(a.age)", 45),
            new IntVal("MIN(a.age)", 20), new DoubleVal("SUM(b.eyeSight)", 69.1),
            new IntVal("COUNT(*)", 14)});
        assertTest(queryStr, sampleTupleForTable, tupleValues);
    }

    @Test
    public void testNodeAsGroupBy() {
        var queryStr = "MATCH (a:PERSON)-[:KNOWS]->(b:PERSON) RETURN b, sum(a.age)";
        var plans = getAllUnfactorizedPlans(queryStr);
        plans.forEach(plan -> {
            plan.setStoreTuples(true);
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            Assertions.assertEquals(6 /*expected number of groups in table*/, table.getTuples().
                size());
            /* tests all nodeVals values to be unique*/
            testUniqueValues(table.getTuples().stream().map(tuple -> tuple.get(0)).collect(
                Collectors.toList()));
            /* tests all sum(a.age) values are as expected */
            var expectedSumVals = new HashSet<>(Arrays.asList(100, 85, 110, 20, 95));
            table.getTuples().forEach(tuple -> {
                var intVal = tuple.get(1);
                Assertions.assertTrue(expectedSumVals.contains(intVal.getInt()));
            });
        });
    }

    @Test
    public void testStringAsGroupBy() {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN b.fName, sum(b.gender), count(*)";
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.fName", DataType.STRING));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("SUM(b.gender)", DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("COUNT(*)", DataType.INT));
        var tupleValues = new ArrayList<Value[]>();
        tupleValues.add(new Value[]{new StringVal("b.fName","Dan"),
            new IntVal("SUM(b.gender)", 6),
            new IntVal("COUNT(*)", 3)});
        tupleValues.add(new Value[]{new StringVal("b.fName","Bob"),
            new IntVal("SUM(b.gender)", 6),
            new IntVal("COUNT(*)", 3)});
        tupleValues.add(new Value[]{new StringVal("b.fName","Farooq"),
            new IntVal("SUM(b.gender)", 2),
            new IntVal("COUNT(*)", 1)});
        tupleValues.add(new Value[]{new StringVal("b.fName","Alice"),
            new IntVal("SUM(b.gender)", 3),
            new IntVal("COUNT(*)", 3)});
        tupleValues.add(new Value[]{new StringVal("b.fName","Carol"),
            new IntVal("SUM(b.gender)", 3),
            new IntVal("COUNT(*)", 3)});
        tupleValues.add(new Value[]{new StringVal("b.fName","Greg"),
            new IntVal("SUM(b.gender)", 2),
            new IntVal("COUNT(*)", 1)});
        assertTest(queryStr, sampleTupleForTable, tupleValues);
    }

    @Test
    public void testDoubleAsGroupBy() {
        var queryStr = "MATCH (a:PERSON)-[e1:STUDYAT]->(b:ORGANISATION) " +
            "RETURN b.mark, sum(a.age), count(*)";
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.mark", DataType.DOUBLE));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("SUM(a.age)", DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("COUNT(*)", DataType.INT));
        var tupleValues = new ArrayList<Value[]>();
        tupleValues.add(new Value[]{new DoubleVal("b.mark",3.7),
            new IntVal("SUM(a.age)", 90),
            new IntVal("COUNT(*)", 3)});
        assertTest(queryStr, sampleTupleForTable, tupleValues);
    }

    @Test
    public void testMultiPart() {
        var queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b, max(e1.year) as yearMax, min(e1.year) as yearMin " +
            "WHERE yearMax < 2017 " +
            "MATCH (c:PERSON)-[e2:WORKAT]->(b) " +
            "WHERE yearMin < e2.year OR yearMax > e2.year " +
            "RETURN b.orgCode, yearMin, yearMax, SUM(c.eyeSight)";
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.orgCode", DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("yearMin", DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("yearMax", DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("SUM(c.eyeSight)", DataType.DOUBLE));
        var tupleValues = new ArrayList<Value[]>();
        tupleValues.add(new Value[]{new IntVal("b.orgCode", 824),
            new IntVal("yearMin", 2010),
            new IntVal("yearMax", 2015),
            new DoubleVal("SUM(c.eyeSight)", 9.5)});
        assertTest(queryStr, sampleTupleForTable, tupleValues);
    }
}