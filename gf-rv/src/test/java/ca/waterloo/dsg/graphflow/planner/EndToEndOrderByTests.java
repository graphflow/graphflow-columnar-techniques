package ca.waterloo.dsg.graphflow.planner;

import ca.waterloo.dsg.graphflow.plan.RegularQueryPlan;
import ca.waterloo.dsg.graphflow.tuple.Table;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.tuple.value.flat.BoolVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.DoubleVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.NodeVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.StringVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class EndToEndOrderByTests extends AbstractEndToEndTests{

    @Test
    public void testOrderByInt() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE b.fName = \"Bob\" " +
            "RETURN a.age ORDER BY a.age";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age", DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{20});
            tupleIntValues.add(new int[]{35});
            tupleIntValues.add(new int[]{45});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            expectedTable.getTuples().forEach(tuple -> {
                for (var i=0; i<tuple.numValues(); ++i) {
                    if (tuple.get(i).getDataType() == DataType.NODE) {
                        tuple.get(i).setNodeType(1);
                    }
                }
            });
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByIntInMultiQuery() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a, b " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN c.age " +
            "ORDER BY c.age DESC";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            // For the 4-clique, 4*3*3 outputs because there are that many 2-paths and the
            // projection does not remove row duplicates.
            // For 8<-7->9 part of the graph there are 4 outputs: 8, 8, 8, 9, 9, 8, and 9, 9
            // We check several outputs. There should be three 0,0's, two 2,5's, and one 8,8's
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("c.age", DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            for (int i = 0; i < 9; ++i) {
                tupleIntValues.add(new int[]{45});
            }
            for (int i = 0; i < 9; ++i) {
                tupleIntValues.add(new int[]{35});
            }
            for (int i = 0; i < 9; ++i) {
                tupleIntValues.add(new int[]{30});
            }
            for (int i = 0; i < 9; ++i) {
                tupleIntValues.add(new int[]{20});
            }
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByIntAnother() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE b.fName = \"Bob\" " +
            "RETURN e1.date, sum(a.age) ORDER BY sum(a.age) DESC";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            Assertions.assertTrue(verifyOrderByInt(plan, "SUM(a.age)"));
        });
    }

    @Test
    public void testOrderByIntInMultiQueryAnother() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a, b " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) " +
            "RETURN e2.date, sum(c.age) " +
            "ORDER BY sum(c.age) DESC";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            Assertions.assertTrue(verifyOrderByIntInMultiQuery(plan, "SUM(c.age)"));
        });
    }

    @Test
    public void testOrderByDouble() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE b.fName = \"Bob\" " +
            "RETURN a.age * 2.0 ORDER BY a.age * 2.0";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age * 2.0",
                DataType.DOUBLE));
            var expectedTable = new Table(sampleTupleForTable);
            expectedTable.add(new Tuple(new Value[]{ new DoubleVal("a.age * 2.0", 40.0) },
                sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{ new DoubleVal("a.age * 2.0", 70.0) },
                sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{ new DoubleVal("a.age * 2.0", 90.0) },
                sampleTupleForTable.getSchema()));
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByDoubleInMultiQuery() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a, b " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) " +
            "RETURN c.age * 2.0 " +
            "ORDER BY c.age * 2.0";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            // see the comment in test testMultiProjection for the output
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("c.age * 2.0",
                DataType.DOUBLE));
            var expectedTable = new Table(sampleTupleForTable);
            for (int i = 0; i < 9; ++i) {
                expectedTable.add(new Tuple(new Value[]{ new DoubleVal("c.age * 2.0", 40.0)},
                    sampleTupleForTable.getSchema()));
            }
            for (int i = 0; i < 9; ++i) {
                expectedTable.add(new Tuple(new Value[]{ new DoubleVal("c.age * 2.0", 60.0)},
                    sampleTupleForTable.getSchema()));
            }
            for (int i = 0; i < 9; ++i) {
                expectedTable.add(new Tuple(new Value[]{ new DoubleVal("c.age * 2.0", 70.0)},
                    sampleTupleForTable.getSchema()));
            }
            for (int i = 0; i < 9; ++i) {
                expectedTable.add(new Tuple(new Value[]{ new DoubleVal("c.age * 2.0", 90.0)},
                    sampleTupleForTable.getSchema()));
            }
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByString() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE b.fName = \"Bob\" " +
            "RETURN a.fName ORDER BY a.fName DESC";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.fName", DataType.
                STRING));
            var expectedTable = new Table(sampleTupleForTable);
            expectedTable.add(new Tuple(new Value[]{new StringVal("a.fName", "Dan")},
                sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{new StringVal("a.fName", "Carol")},
                sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{new StringVal("a.fName", "Alice")},
                sampleTupleForTable.getSchema()));
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByStringInMultiQuery() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a, b " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN c.fName " +
            "ORDER BY c.fName";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            // see the comment in test testMultiProjection for the output
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("c.fName", DataType.
                STRING));
            var expectedTable = new Table(sampleTupleForTable);
            for (int i = 0; i < 9; ++i) {
                expectedTable.add(new Tuple(new Value[]{ new StringVal("c.fName", "Alice")},
                    sampleTupleForTable.getSchema()));
            }
            for (int i = 0; i < 9; ++i) {
                expectedTable.add(new Tuple(new Value[]{ new StringVal("c.fName", "Bob") },
                    sampleTupleForTable.getSchema()));
            }
            for (int i = 0; i < 9; ++i) {
                expectedTable.add(new Tuple(new Value[]{ new StringVal("c.fName", "Carol") },
                    sampleTupleForTable.getSchema()));
            }
            for (int i = 0; i < 9; ++i) {
                expectedTable.add(new Tuple(new Value[]{ new StringVal("c.fName", "Dan") },
                    sampleTupleForTable.getSchema()));
            }
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByBoolean() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE b.fName = \"Bob\" " +
            "RETURN a.isWorker ORDER BY a.isWorker DESC";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(
                ValueFactory.getFlatValueForDataType("a.isWorker", DataType.BOOLEAN));
            var expectedTable = new Table(sampleTupleForTable);
            expectedTable.add(new Tuple(new Value[]{ new BoolVal("a.isWorker", true)
            }, sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{ new BoolVal("a.isWorker", true)
            }, sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{ new BoolVal("a.isWorker", false)
            }, sampleTupleForTable.getSchema()));
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByBooleanInMultiQuery() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a, b " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN c.isStudent " +
            "ORDER BY c.isStudent DESC";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            // see the comment in test testMultiProjection for the output
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("c.isStudent", DataType.
                BOOLEAN));
            var expectedTable = new Table(sampleTupleForTable);
            for (int i = 0; i < 18; ++i) {
                expectedTable.add(new Tuple(new Value[]{ new BoolVal("c.isStudent", true) },
                    sampleTupleForTable.getSchema()));
            }
            for (int i = 0; i < 18; ++i) {
                expectedTable.add(new Tuple(new Value[]{ new BoolVal("c.isStudent", false) },
                    sampleTupleForTable.getSchema()));
            }
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByAliasedExpression() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE b.fName = \"Bob\" " +
            "RETURN e1.date, sum(a.age) AS s ORDER BY s DESC";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            Assertions.assertTrue(verifyOrderByInt(plan, "s"));
        });
    }

    @Test
    public void testOrderByAliasedExpressionInMultiQuery() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a, b " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) " +
            "RETURN e2.date, sum(c.age) AS s " +
            "ORDER BY s DESC";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            Assertions.assertTrue(verifyOrderByIntInMultiQuery(plan, "s"));
        });
    }

    private boolean verifyOrderByInt(RegularQueryPlan plan, String aliasedColumnName) {
        plan.init(graphTinySnb);
        plan.execute();
        var table = plan.getOutputTable();
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("e1.date",
            DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType(aliasedColumnName,
            DataType.INT));
        var tupleIntValues = new ArrayList<int[]>();
        tupleIntValues.add(new int[]{1234567892, 65});
        tupleIntValues.add(new int[]{1234567890, 35});
        var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
        return expectedTable.isSameAndInSameOrder(table);
    }

    @Test
    public void testOrderByTwoProperties() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE b.fName = \"Bob\" " +
            "RETURN a, a.fName, a.isWorker, a.age ORDER BY a.isWorker, a.age";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a", DataType.NODE));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.fName", DataType.
                STRING));
            sampleTupleForTable.append(
                ValueFactory.getFlatValueForDataType("a.isWorker", DataType.BOOLEAN));
            var expectedTable = new Table(sampleTupleForTable);
            expectedTable.add(new Tuple(new Value[]{
                new NodeVal("a", 0, 0),
                new IntVal("a.age", 35),
                new StringVal("a.fName", "Alice"),
                new BoolVal("a.isWorker", false)
            }, sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{
                new NodeVal("a", 0, 3),
                new IntVal("a.age", 20),
                new StringVal("a.fName", "Dan"),
                new BoolVal("a.isWorker", true)
            }, sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{
                new NodeVal("a", 0, 2),
                new IntVal("a.age", 45),
                new StringVal("a.fName", "Carol"),
                new BoolVal("a.isWorker", true)
            }, sampleTupleForTable.getSchema()));

            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByTwoPropertiesOneAscOneDesc() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE b.fName = \"Bob\" " +
            "RETURN a, a.fName, a.isWorker, a.age ORDER BY a.isWorker, a.age DESC";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a", DataType.NODE));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.fName", DataType.
                STRING));
            sampleTupleForTable.append(
                ValueFactory.getFlatValueForDataType("a.isWorker", DataType.BOOLEAN));
            var expectedTable = new Table(sampleTupleForTable);
            expectedTable.add(new Tuple(new Value[]{
                new NodeVal("a", 0, 0),
                new IntVal("a.age", 35),
                new StringVal("a.fName", "Alice"),
                new BoolVal("a.isWorker", false)
            }, sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{
                new NodeVal("a", 0, 2),
                new IntVal("a.age", 45),
                new StringVal("a.fName", "Carol"),
                new BoolVal("a.isWorker", true)
            }, sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{
                new NodeVal("a", 0, 3),
                new IntVal("a.age", 20),
                new StringVal("a.fName", "Dan"),
                new BoolVal("a.isWorker", true)
            }, sampleTupleForTable.getSchema()));

            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByTwoPropertiesInMultiQuery() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a, b " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN a.age, c.fName ORDER BY c.fName, a.age";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var expectedTable = getExpectedTableOfAgeAndNameOfWhoKnowsWho("a.age", "c.fName");
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByThreeProperties() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE b.fName = \"Bob\" or " +
            "b.fName=\"Alice\" " +
            "RETURN a.fName, a.isWorker, b.fName ORDER BY a.isWorker, b.fName, a.fName";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.fName",
                DataType.STRING));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.fName",
                DataType.STRING));
            sampleTupleForTable.append(
                ValueFactory.getFlatValueForDataType("a.isWorker", DataType.BOOLEAN));
            var expectedTable = new Table(sampleTupleForTable);
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Bob", false, "Alice", sampleTupleForTable));
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Alice", false, "Bob", sampleTupleForTable));
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Carol", true, "Alice", sampleTupleForTable));
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Dan", true, "Alice", sampleTupleForTable));
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Carol", true, "Bob", sampleTupleForTable));
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Dan", true, "Bob", sampleTupleForTable));
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByThreePropertiesTwoAscOneDesc() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE b.fName = \"Bob\" or " +
            "b.fName=\"Alice\" " +
            "RETURN a.fName, a.isWorker, b.fName ORDER BY  a.isWorker, b.fName, a.fName DESC";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.fName",
                DataType.STRING));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.fName",
                DataType.STRING));
            sampleTupleForTable.append(
                ValueFactory.getFlatValueForDataType("a.isWorker", DataType.BOOLEAN));
            var expectedTable = new Table(sampleTupleForTable);
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Bob", false, "Alice", sampleTupleForTable));
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Alice", false, "Bob", sampleTupleForTable));
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Dan", true, "Alice", sampleTupleForTable));
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Carol", true, "Alice", sampleTupleForTable));
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Dan", true, "Bob", sampleTupleForTable));
            expectedTable.add(getTupleForTestPeopleKnowsAliceAndBob(
                "Carol", true, "Bob", sampleTupleForTable));
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    private Tuple getTupleForTestPeopleKnowsAliceAndBob(String aName, boolean isWorker,
        String bName, Tuple sampleTupleForTable) {
        var newTuple =  new Tuple(new Value[]{
            new StringVal("a.fName", aName),
            new BoolVal("a.isWorker", isWorker),
            new StringVal("b.fName", bName),
        }, sampleTupleForTable.getSchema());
        return newTuple;
    }

    @Test
    public void testOrderByMultipleAliasedExpressions() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE b.fName = \"Bob\" " +
            "RETURN a.isWorker AS worker, a.age AS age ORDER BY worker, age DESC";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("age", DataType.INT));
            sampleTupleForTable.append(
                ValueFactory.getFlatValueForDataType("worker", DataType.BOOLEAN));
            var expectedTable = new Table(sampleTupleForTable);
            expectedTable.add(new Tuple(new Value[]{
                new IntVal("age", 35),
                new BoolVal("worker", false)
            }, sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{
                new IntVal("age", 45),
                new BoolVal("worker", true)
            }, sampleTupleForTable.getSchema()));
            expectedTable.add(new Tuple(new Value[]{
                new IntVal("age", 20),
                new BoolVal("worker", true)
            }, sampleTupleForTable.getSchema()));
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    @Test
    public void testOrderByMultipleAliasedExpressionsInMultiQuery() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a, b " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN a.age as age, c.fName as name " +
            "ORDER BY name, age";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var expectedTable = getExpectedTableOfAgeAndNameOfWhoKnowsWho("age", "name");
            Assertions.assertTrue(expectedTable.isSameAndInSameOrder(table));
        });
    }

    private boolean verifyOrderByIntInMultiQuery(RegularQueryPlan plan, String aliasedColumnName) {
        plan.init(graphTinySnb);
        plan.execute();
        var table = plan.getOutputTable();
        // see the comment in test testMultiProjection for the output
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("e2.date",
            DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType(aliasedColumnName,
            DataType.INT));
        var tupleIntValues = new ArrayList<int[]>();
        tupleIntValues.add(new int[]{1234567890, 600});
        tupleIntValues.add(new int[]{1234567892, 375});
        tupleIntValues.add(new int[]{1234567893, 195});
        var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
        return expectedTable.isSameAndInSameOrder(table);
    }

    private Table addAgeAndFNameToExpectedTable(Table expectedTable, Tuple sampleTupleForTable,
        String ageProperty, String nameProperty, int age, String name, int repeat) {
        for (int i = 0; i < repeat; ++i) {
            expectedTable.add(new Tuple(new Value[] {
                new IntVal(ageProperty, age),
                new StringVal(nameProperty, name)
            }, sampleTupleForTable.getSchema()));
        }
        return expectedTable;
    }

    private Table getExpectedTableOfAgeAndNameOfWhoKnowsWho(String ageProperty,
        String nameProperty) {
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType(nameProperty,
            DataType.STRING));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType(ageProperty,
            DataType.INT));
        var expectedTable = new Table(sampleTupleForTable);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 20, "Alice", 2);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 30, "Alice", 2);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 35, "Alice", 3);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 45, "Alice", 2);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 20, "Bob", 2);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 30, "Bob", 3);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 35, "Bob", 2);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 45, "Bob", 2);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 20, "Carol", 2);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 30, "Carol", 2);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 35, "Carol", 2);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 45, "Carol", 3);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 20, "Dan", 3);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 30, "Dan", 2);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 35, "Dan", 2);
        addAgeAndFNameToExpectedTable(expectedTable, sampleTupleForTable,
            ageProperty, nameProperty, 45, "Dan", 2);
        return expectedTable;
    }
}
