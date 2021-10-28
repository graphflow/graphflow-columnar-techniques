package ca.waterloo.dsg.graphflow.planner;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.QueryParser;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.planner.enumerators.RegularQueryPlanEnumerator;
import ca.waterloo.dsg.graphflow.tuple.Table;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.StringVal;
import ca.waterloo.dsg.graphflow.util.DataLoader;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * Simple unit tests that verify, for several queries, that the output of every plan that the
 * QueryPlanFactory generates for each query is correct.
 */
public class EndToEndSinglePartQueryTests extends BaseSingleAndMultiPartEndToEndTests {

    @Test
    public void testOneChain() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN *", 14);
    }

    @Test
    public void testTwoChain() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), (b:PERSON)-[e2:KNOWS]->(c:PERSON)" +
            " RETURN *", 4 * 3 * 3);
    }

    @Test
    public void testThreeChain() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), (b:PERSON)-[e2:KNOWS]->(c:PERSON), " +
            "(c:PERSON)-[e3:KNOWS]->(d:PERSON) RETURN *", 4 * 3 * 3 * 3);
    }

    @Test
    public void testScanWithSimpleFilter() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.gender = 1 AND" +
            " b.gender = 2 RETURN *", 6);
    }

    @Test
    public void testScanWithSimpleFilterParenthesized() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE (a.gender/2 <= 0.5) AND" +
            " (b.gender*3.5 = 7.0) RETURN *", 6);
    }

    @Test
    public void testMultiExtend() {
        // Alice: 3*3, Bob: 2*2 + 1*1 + Carol,Dan: 1*1 + 1*1 + 1*1, Elizabeth: 2*2
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) , (a:PERSON)-[e2:KNOWS]->(c:PERSON)" +
            " WHERE e1.date = e2.date RETURN *", (3 * 3) + (2 * 2 + 1 * 1) + 2 * (1 * 1 + 1 * 1 + 1 * 1) + (2 * 2));
    }

    @Test
    public void testRelValsUniqueness() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN a, e1, b";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            Assertions.assertEquals(14 /*number of edges in KNOWS*/, table.getTuples().size());
            var relVals = table.getTuples().stream().map(tuple -> tuple.get(tuple.getIdx("e1")))
                .collect(Collectors.toList());
            testUniqueValues(relVals);
        });
    }

    @Test
    public void testProjection() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), " +
            "(a:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN b.age, c.age";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            // For the 4-clique, 4*3*3 outputs because there are that many 2-paths and the
            // projection does not remove row duplicates.
            // For 8<-7->9 part of the graph there are 4 outputs: 8, 8, 8, 9, 9, 8, and 9, 9
            Assertions.assertEquals(4 * 3 * 3 + 4, table.getTuples().size());
            // We check several outputs. There should be three 0,0's, two 2,5's, and one 8,8's
            // The table is very large, so Table.isSame method will not be used.
            var num00 = 0;
            var num25 = 0;
            var num88 = 0;
            for (var tuple : table.getTuples()) {
                Assertions.assertEquals(2, tuple.numValues());
                if (35 == tuple.get(0).getInt() && 35 == tuple.get(1).getInt()) {
                    num00++;
                } else if (30 == tuple.get(0).getInt() && 20 == tuple.get(1).getInt()) {
                    num25++;
                } else if (25 == tuple.get(0).getInt() && 25 == tuple.get(1).getInt()) {
                    num88++;
                }
            }
            Assertions.assertEquals(3, num00);
            Assertions.assertEquals(2, num25);
            Assertions.assertEquals(1, num88);
        });
    }

    @Test
    public void testBooleanPropertyVariable() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.isStudent RETURN *";
        // Alice and Bob know 3 people each and Farooq does not know any one.
        testAllPlans(queryStr, 6);
    }

    @Test
    public void testNOTBooleanPropertyVariable() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE NOT a.isStudent RETURN *";
        // Carol and Dan know 3 each, and Elizabeth knows 2 people.
        testAllPlans(queryStr, 8);
    }

    @Test
    public void testNOTSimple() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE NOT a.fName <> \"Alice\" " +
            "RETURN a.age, b.age";
        // We expect to return only Alice's friends
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.age",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{35, 30});
            tupleIntValues.add(new int[]{35, 45});
            tupleIntValues.add(new int[]{35, 20});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testOR() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.fName = \"Alice\" OR a.fName = \"Bob\" OR a.gender = 1 " +
            "RETURN a.age, b.age";
        verifyAliceOrBobOrFemale(queryStr);
    }

    private void verifyAliceOrBobOrFemale(String queryStr) {
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.age",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{35, 30});
            tupleIntValues.add(new int[]{35, 45});
            tupleIntValues.add(new int[]{35, 20});
            tupleIntValues.add(new int[]{30, 35});
            tupleIntValues.add(new int[]{30, 45});
            tupleIntValues.add(new int[]{30, 20});
            tupleIntValues.add(new int[]{45, 35});
            tupleIntValues.add(new int[]{45, 30});
            tupleIntValues.add(new int[]{45, 20});
            tupleIntValues.add(new int[]{20, 25});
            tupleIntValues.add(new int[]{20, 40});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testORBooleanProperty() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.isStudent OR " +
            "a.isWorker RETURN *";
        testAllPlans(queryStr, 14);
    }

    @Test
    public void testNOTOR() {
        // Reverses the predicates in the testOR and adds NOT in front
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE NOT a.fName <> \"Alice\" OR NOT a.fName <> \"Bob\" OR NOT a.gender <> 1 " +
            "RETURN a.age, b.age";
        verifyAliceOrBobOrFemale(queryStr);
    }

    @Test
    public void testORAND() {
        // This predicate is parsed as (a.fName = "Alice") OR (a.age < 21 AND a.gender = 2) OR
        // (a.fName = "Elizabeth"). The output contains edges of Alice, Elizabeth, and Dan who
        // is male with age < 21.
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.fName = \"Alice\" OR a.age < 21 AND a.gender = 2 OR a.fName = \"Elizabeth\" " +
            "RETURN a.age, b.age";
        verifyTestORAND(queryStr);
    }

    @Test
    public void testORANDParenthesized() {
        // This predicate is parsed as (a.fName = "Alice" OR a.age < 21) AND (a.gender = 2 OR
        // a.fName = "Elizabeth"). Dan who is both male and with age < 21 and Elizabeth who
        // is < 21 and has name Elizabeth pass this test.
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE (a.fName = \"Alice\" OR a.age < 21) AND (a.gender = 2 OR a.fName = " +
            "\"Elizabeth\") RETURN a.age, b.age";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.age",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{20, 25});
            tupleIntValues.add(new int[]{20, 40});
            tupleIntValues.add(new int[]{20, 35});
            tupleIntValues.add(new int[]{20, 30});
            tupleIntValues.add(new int[]{20, 45});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testNOTORAND() {
        // Reverses the predicates in the testORAND and adds NOT in front
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE NOT a.fName <> \"Alice\" OR NOT a.age >= 21 AND NOT a.gender <> 2 OR NOT a" +
            ".fName <> \"Elizabeth\" " + "RETURN a.age, b.age";
        verifyTestORAND(queryStr);
    }

    private void verifyTestORAND(String queryStr) {
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.age",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{35, 30});
            tupleIntValues.add(new int[]{35, 45});
            tupleIntValues.add(new int[]{35, 20});
            tupleIntValues.add(new int[]{20, 25});
            tupleIntValues.add(new int[]{20, 40});
            tupleIntValues.add(new int[]{20, 35});
            tupleIntValues.add(new int[]{20, 30});
            tupleIntValues.add(new int[]{20, 45});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    /* This tests uses the "webgraph" database and not the usual "tiny-snb" for testing. */
    /* TODO: The way this test runs is not correct, since it cannot reuse plan enumeration
        functions and has to do query parsing and enumeration separately. In the future, we want
        each test to specify which graph it is using that should be passed to plan enumerator
        functions as parameters.
        */
    @Test
    public void testRelAndNodePropertyOfSameName() {
        var graphWebGraph = DataLoader.getDataset("webgraph").graph;
        var queryStr = "MATCH (a:WEBPAGE)-[e1:LINK]->(b:WEBPAGE) " +
            "RETURN b.creationTimestamp, e1.creationTimestamp";
        var query = (RegularQuery) QueryParser.parseQuery(queryStr, graphWebGraph.getGraphCatalog());
        var enumerator = new RegularQueryPlanEnumerator(query, graphWebGraph);
        var plans = enumerator.enumeratePlansForQuery();
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.creationTimestamp",
            DataType.INT));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("e1.creationTimestamp",
            DataType.STRING));
        var tupleValues = new ArrayList<Value[]>();
        tupleValues.add(new Value[]{new IntVal("b.creationTimestamp", 1237859355),
            new StringVal("e1.creationTimestamp", "7520868002")});
        tupleValues.add(new Value[]{new IntVal("b.creationTimestamp", 1385728573),
            new StringVal("e1.creationTimestamp", "9249275239")});
        tupleValues.add(new Value[]{new IntVal("b.creationTimestamp", 1385728573),
            new StringVal("e1.creationTimestamp", "3794693294")});
        tupleValues.add(new Value[]{new IntVal("b.creationTimestamp", 417489358),
            new StringVal("e1.creationTimestamp", "5820562752")});
        tupleValues.add(new Value[]{new IntVal("b.creationTimestamp", 417489358),
            new StringVal("e1.creationTimestamp", "5820957157")});
        tupleValues.add(new Value[]{new IntVal("b.creationTimestamp", 1237859355),
            new StringVal("e1.creationTimestamp", "4269140985")});
        tupleValues.add(new Value[]{new IntVal("b.creationTimestamp", 835792503),
            new StringVal("e1.creationTimestamp", "4325627592")});
        tupleValues.add(new Value[]{new IntVal("b.creationTimestamp", 749301014),
            new StringVal("e1.creationTimestamp", "1573587425")});
        tupleValues.add(new Value[]{new IntVal("b.creationTimestamp", 1237859355),
            new StringVal("e1.creationTimestamp", "5257328570")});
        tupleValues.add(new Value[]{new IntVal("b.creationTimestamp", 417489358),
            new StringVal("e1.creationTimestamp", "7422957084")});
        var expectedTable = Table.constructTableForTest(sampleTupleForTable, tupleValues);
        plans.forEach(plan -> {
            plan.setStoreTuples(true);
            plan.init(graphWebGraph);
            plan.execute();
            var table = plan.getOutputTable();
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testRelProjection() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), " +
            "(a:PERSON)-[e2:WORKAT]->(c:ORGANISATION) RETURN e1.date, e2.year";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            // a can be 3 distinct values: _:3, _:5, _:7.
            // for a = _:3 there are 3 rows. for a = _:5, there are 2 rows.
            // for a = _:7, there are 2 rows.
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("e1.date",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("e2.year",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{1234567890, 2015});
            tupleIntValues.add(new int[]{1234567892, 2015});
            tupleIntValues.add(new int[]{1234567893, 2015});
            tupleIntValues.add(new int[]{1234567890, 2010});
            tupleIntValues.add(new int[]{1234567892, 2010});
            tupleIntValues.add(new int[]{1234567893, 2010});
            tupleIntValues.add(new int[]{1234567897, 2015});
            tupleIntValues.add(new int[]{1234567897, 2015});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testBooleanProperty() {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.isWorker = true " +
            "RETURN a.age, b.age";
        // Carol and Dan with 3 friends and Elizabeth with with 2 friends have isWorker = true
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.age",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{45, 35});
            tupleIntValues.add(new int[]{45, 30});
            tupleIntValues.add(new int[]{45, 20});
            tupleIntValues.add(new int[]{20, 35});
            tupleIntValues.add(new int[]{20, 30});
            tupleIntValues.add(new int[]{20, 45});
            tupleIntValues.add(new int[]{20, 25});
            tupleIntValues.add(new int[]{20, 40});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testArithmeticAdditionExpressionInWhere() {
        // Only Carol, whose age is 45 should pass this test
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age + 5 > 49" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
        queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age + 5.0 > 49" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
    }

    @Test
    public void testArithmeticSubtractionExpressionInWhere() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age - 4 >= 41" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
        queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age - 4.0 >= 41.0" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
    }

    @Test
    public void testArithmeticMultiplyExpressionInWhere() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age * 2 > 88" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
        queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age * 2.0 > 88.0" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
    }

    @Test
    public void testArithmeticModuloExpressionInWhere() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age % 44 = 1" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
        queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age % 44.0 = 1" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
    }

    @Test
    public void testArithmeticDivisionExpressionInWhere() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age / 2 = 22.5" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
    }

    @Test
    public void testArithmeticUnaryNegationExpressionInWhere() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE -1 * a.age = -45" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
    }

    @Test
    public void testArithmeticPowerExpressionInWhere() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age ^ 2 = 2025" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
        queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age ^ 2.0 = 2025" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
    }

    private void verifyOnlyCarolExists(String queryStr) {
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.age",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{45, 35});
            tupleIntValues.add(new int[]{45, 30});
            tupleIntValues.add(new int[]{45, 20});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testComplexArithmeticExpressionInWhere1() {
        // The parenthesization here does not matter because * and / are commutative.
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE -1 * a.age / -2.0 = 22.5" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
        // The parenthesization here is (-1 * a.age) - 2 because multiplication takes precedence
        // over addition.
        queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE -1 * a.age - 2= -47" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
    }

    @Test
    public void testComplexArithmeticExpressionInWhere1Parenthesized() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE -1 * (a.age - 2) = -47" +
            "RETURN a.age, b.age";
        testAllPlans(queryStr, 0);
        queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE -1 * (a.age - 2) = -43" +
            "RETURN a.age, b.age";
        verifyOnlyCarolExists(queryStr);
    }

    @Test
    public void testComplexArithmeticExpressionInWhere2() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.age * b.age = 45*20 OR a.age * b.age = 45*30 OR a.age * b.age = 45*35" +
            "RETURN a.age, b.age";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.age",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{45, 35});
            tupleIntValues.add(new int[]{35, 45});
            tupleIntValues.add(new int[]{45, 30});
            tupleIntValues.add(new int[]{30, 45});
            tupleIntValues.add(new int[]{45, 20});
            tupleIntValues.add(new int[]{20, 45});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testPropertyProjection() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), " +
            "(a:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN e1.date, e2.date";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            // For the 4-clique, 4*3*3 outputs because there are that many 2-paths and the
            // projection does not remove row duplicates.
            // For 8<-7->9 part of the graph there are 4 outputs: 8, 8, 8, 9, 9, 8, and 9, 9
            Assertions.assertEquals(4 * 3 * 3 + 4, table.getTuples().size());
            // We check several outputs. There should be twelve 1234567890,1234567890's,
            // four 1234567890,12345678925's and two 1234567893,1234567892's
            // The table is very large, so Table.isSame method will not be used.
            var num00 = 0;
            var num02 = 0;
            var num32 = 0;
            for (var tuple : table.getTuples()) {
                Assertions.assertEquals(2, tuple.numValues());
                if (1234567890 == tuple.get(0).getInt() && 1234567890 == tuple.get(1).getInt()) {
                    num00++;
                } else if (1234567890 == tuple.get(0).getInt() &&
                    1234567892 == tuple.get(1).getInt()) {
                    num02++;
                } else if (1234567893 == tuple.get(0).getInt() &&
                    1234567892 == tuple.get(1).getInt()) {
                    num32++;
                }
            }
            Assertions.assertEquals(12, num00);
            Assertions.assertEquals(4, num02);
            Assertions.assertEquals(2, num32);
        });
    }

    @Test
    public void testCountStarSinglePart() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), " +
            "(b:PERSON)-[e2:KNOWS]->(c:PERSON), (c:PERSON)-[e3:KNOWS]->(d:PERSON) " +
            "WHERE e2.date = e3.date RETURN count(*)";
        testCountStar(queryStr);
    }

    @Test
    public void testMinDate() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN b.age, min(e1.date)";
        testMinDate(queryStr);
    }

    @Test
    public void testMaxAge() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN b.age, max(a.age)";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.age",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("MAX(a.age)",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{35, 45});
            tupleIntValues.add(new int[]{30, 45});
            tupleIntValues.add(new int[]{45, 35});
            tupleIntValues.add(new int[]{20, 45});
            tupleIntValues.add(new int[]{25, 20});
            tupleIntValues.add(new int[]{40, 20});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testExpressionInProjection() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.fName = \"Carol\" " +
            "RETURN a.age, b.age*b.age+a.age";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            var expressionColumnName =
                !table.getSampleTupleForSchema().get(0).getVariableName().equals("a.age") ?
                    table.getSampleTupleForSchema().get(0).getVariableName() :
                    table.getSampleTupleForSchema().get(1).getVariableName();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType(expressionColumnName,
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{45, 35 * 35 + 45});
            tupleIntValues.add(new int[]{45, 30 * 30 + 45});
            tupleIntValues.add(new int[]{45, 20 * 20 + 45});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testExpressionInAggregation() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.fName = \"Carol\" " +
            "RETURN a.age, 2*sum(2*b.age - 1)";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            var expressionColumnName =
                !table.getSampleTupleForSchema().get(0).getVariableName().equals("a") ?
                    table.getSampleTupleForSchema().get(0).getVariableName() :
                    table.getSampleTupleForSchema().get(1).getVariableName();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType(expressionColumnName,
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{45, 2 * ((2 * 35 - 1) + (2 * 30 - 1) + (2 * 20 - 1))});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testSumYear() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "RETURN b.orgCode, sum(e1.year)";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.orgCode",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("SUM(e1.year)",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{934, 2015});
            tupleIntValues.add(new int[]{824, 4025});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testGroupByRelProperty() {
        String queryStr = "MATCH (a:PERSON)-[e1:STUDYAT]->(b:ORGANISATION) " +
            "RETURN e1.year, sum(a.age)";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("e1.year", DataType.
                INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("SUM(a.age)", DataType.
                INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{2020, 55});
            tupleIntValues.add(new int[]{2021, 35});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testGroupByNodeProperty() {
        String queryStr = "MATCH (a:PERSON)-[e1:STUDYAT]->(b:ORGANISATION) " +
            "RETURN a.gender, min(e1.year)";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.gender",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("MIN(e1.year)",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{1, 2021});
            tupleIntValues.add(new int[]{2, 2020});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testStartsWithInPredicate() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.fName STARTS " +
            "WITH \"A\" RETURN *";
        testAllPlans(queryStr, 3);
    }

    @Test
    public void testEndsWithInPredicate() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.fName ENDS " +
            "WITH \"eth\" RETURN *";
        testAllPlans(queryStr, 2);
    }

    @Test
    public void testContainsInPredicate() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.fName CONTAINS \"o\" " +
            "RETURN *";
        testAllPlans(queryStr, 6);
    }

    @Test
    public void testSkip() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.isStudent OR " +
            "a.isWorker RETURN * SKIP 1";
        testAllPlans(queryStr, 13);
    }

    @Test
    public void testSkipNumberExceedTotalTuples() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.isStudent OR " +
            "a.isWorker RETURN * SKIP 100";
        testAllPlans(queryStr, 0);
    }

    @Test
    public void testLimit() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.isStudent OR " +
            "a.isWorker RETURN * LIMIT 1";
        testAllPlans(queryStr, 1);
    }

    @Test
    public void testLimitNumberExceedTotalTuples() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.isStudent OR " +
            "a.isWorker RETURN * LIMIT 1000";
        testAllPlans(queryStr, 14);
    }

    @Test
    public void testSkipAndLimit() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.isStudent OR " +
            "a.isWorker RETURN * SKIP 2 LIMIT 7";
        testAllPlans(queryStr, 7);
    }

    @Test
    public void testSkipAndLimitExceedTotalTuples() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.isStudent OR " +
            "a.isWorker RETURN * SKIP 2 LIMIT 100";
        testAllPlans(queryStr, 12);
    }

    @Test
    public void testCannotAliasNodeOrRel() {
        Assertions.assertThrows(MalformedQueryException.class, () -> {
            getAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN a AS l").forEach(plan -> {
                plan.init(graphTinySnb);
                plan.execute();
            });
        });
    }

    @Test
    public void test() {
        String queryStr = "MATCH (a:PERSON)-[e1:STUDYAT]->(b:ORGANISATION) " +
            "RETURN a.gender, min(e1.year)";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.gender",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("MIN(e1.year)",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{1, 2021});
            tupleIntValues.add(new int[]{2, 2020});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }
}