package ca.waterloo.dsg.graphflow.planner;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.tuple.Table;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * End to end unit tests for multi-part queries, i.e., those using the WITH clause.
 */
public class EndToEndMultiPartQueryTests extends BaseSingleAndMultiPartEndToEndTests {

    @Test
    public void testTwoChainInTwoParts() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH * " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN *", 4 * 3 * 3);
        testAllPlans("MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) WITH * " +
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN *", 4 * 3 * 3);
    }

    @Test
    public void testThreeChainInTwoParts() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH * " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON), (c:PERSON)-[e3:KNOWS]->(d:PERSON) RETURN *",
            4 * 3 * 3 * 3);
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), (b:PERSON)-[e2:KNOWS]->(c:PERSON)" +
            "WITH * MATCH (c:PERSON)-[e3:KNOWS]->(d:PERSON) RETURN *", 4 * 3 * 3 * 3);

        testAllPlans("MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON), (c:PERSON)-[e3:KNOWS]->(d:PERSON)" +
                "WITH * MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN *", 4 * 3 * 3 * 3);
        testAllPlans("MATCH (c:PERSON)-[e3:KNOWS]->(d:PERSON)" +
            "WITH * MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), (b:PERSON)-[e2:KNOWS]->(c:PERSON)" +
            "RETURN *", 4 * 3 * 3 * 3);
    }

    @Test
    public void testThreeChainInThreeParts() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH * " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) WITH * " +
            "MATCH (c:PERSON)-[e3:KNOWS]->(d:PERSON) RETURN *", 4 * 3 * 3 * 3);
        testAllPlans("MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) WITH * " +
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH * " +
            "MATCH (c:PERSON)-[e3:KNOWS]->(d:PERSON) RETURN *", 4 * 3 * 3 * 3);
        testAllPlans("MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) WITH * " +
            "MATCH (c:PERSON)-[e3:KNOWS]->(d:PERSON) WITH * " +
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN *", 4 * 3 * 3 * 3);
        testAllPlans("MATCH (c:PERSON)-[e3:KNOWS]->(d:PERSON) WITH * " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) WITH * " +
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN *", 4 * 3 * 3 * 3);
    }

    @Test
    public void testFilterOnCountStarAggregatedValue() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount > 1 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
    }

    @Test
    public void testFilterOnCountStarAggregatedValueWithArithmeticAddition() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount + 1 > 2 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
        queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount + 1.0 > 2 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
    }

    @Test
    public void testFilterOnCountStarAggregatedValueWithArithmeticSubtraction() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount - 1 > 0 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
        queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount - 1.0 > 0.0 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
    }

    @Test
    public void testFilterOnCountStarAggregatedValueWithArithmeticMultiply() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount * 3 > 5 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
        queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount * 3.0 > 5.0 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
    }

    @Test
    public void testFilterOnCountStarAggregatedValueWithArithmeticModulo() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount % 2 = 0 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
        queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount % 2.0 = 0 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
    }

    @Test
    public void testFilterOnCountStarAggregatedValueWithArithmeticPower() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount ^ 3 = 8 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
        queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount ^ 3.0 = 8 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
    }

    @Test
    public void testComplexArithmeticExpressionInWithWhere1() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE bCount * bCount * bCount = 8 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
        queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, count(*) as bCount " +
            "WHERE -1 * bCount ^ 3.0 = -8 RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
    }

    @Test
    public void testComplexArithmeticExpressionInWithWhere2() {
        // Carol, Dan, and Elizabeth works with 1 organization and only Carol has age 45.
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH a.age as age, count(*) as aCount " +
            "WHERE age * aCount = 45 RETURN *";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("age",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("aCount", DataType.
                INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{45, 1});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testExpressionInProjection() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.fName = \"Carol\" " +
            "WITH a, b.age*b.age+a.age AS ageArithmetics " +
            "MATCH (a)-[e2:WORKAT]->(c:ORGANISATION) " +
            "WHERE ageArithmetics > 35*35+45 - 1" +
            "RETURN a.age, ageArithmetics, c.orgCode";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("ageArithmetics",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("c.orgCode", DataType.
                INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{45, 35*35+45, 934});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testExpressionInAggregation() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WHERE a.fName = \"Carol\" " +
            "WITH a, 2*sum(2*b.age - 1) AS ageArithmetics " +
            "MATCH (a)-[e2:WORKAT]->(c:ORGANISATION) " +
            "WHERE ageArithmetics > 334 - 1" +
            "RETURN a.age, ageArithmetics, c.orgCode";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("a.age", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("ageArithmetics",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("c.orgCode", DataType.
                INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{45, 2*((2*35-1)+(2*30-1)+(2*20-1)), 934});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testFilterOnCountStarAndSummationAggregatedValue() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b, sum(e1.year) as yearSum " +
            "WHERE yearSum > 2000 " +
            "MATCH (c:PERSON)-[e2:WORKAT]->(b)" +
            "WITH b.orgCode as code, count(*) AS bCount " +
            "WHERE bCount > 1 " +
            "RETURN *";
        testCountStarTestWithB6BCount2Output(queryStr);
    }

    private void testCountStarTestWithB6BCount2Output(String queryStr) {
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            // CsWork with ID 4 has 1 person working at it.
            // DEsWork with ID 6 has 2 people working at it.
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("code", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("bCount", DataType.
                INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{824, 2});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testFilterOnSummationAggregatedValueOneResult() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, sum(e1.year) as yearSum " +
            "WHERE yearSum > 2020 RETURN *";
//        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
//            "RETURN a, b, b.orgCode as code, e1.year";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
//            System.out.println(plan);
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("code", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("yearSum", DataType.
                INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{824, 2010+2015});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
//            table.printTable();
//            expectedTable.printTable();
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testFilterOnPredicateOnlyOnInputSchema() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b, min(e1.year) as yearMin " +
            "MATCH (c:PERSON)-[e2:WORKAT]->(b) " +
            "WHERE yearMin > 2010 " +
            "RETURN b.orgCode, c.age, yearMin";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            System.out.println(plan);
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.orgCode", DataType.
                INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("c.age", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("yearMin",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{934, 45, 2015});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            table.printTable();
            expectedTable.printTable();
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testFilterOnPredicateOnInputSchemaAndMatch() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b, min(e1.year) as yearMin " +
            "MATCH (c:PERSON)-[e2:WORKAT]->(b:ORGANISATION) " +
            "WHERE yearMin < e2.year " +
            "RETURN b.orgCode, c.age, yearMin, e2.year";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.orgCode",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("c.age",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("yearMin",
                DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("e2.year",
                DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{824, 20, 2010, 2015});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testWithWhereANDORClauses1() {
        // Only ABFsUni has 3 attendees with sum of 6061, so the output should be empty
        String queryStr = "MATCH (a:PERSON)-[e1:STUDYAT]->(b:ORGANISATION) " +
            "WITH b, sum(e1.year) as yearSum " +
            "WHERE b.name = \"CsWork\" AND yearSum > 5000 RETURN *";
        verifyTestWithWhereANDORClauses1(queryStr);
    }

    private void verifyTestWithWhereANDORClauses1(String queryStr) {
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            Assertions.assertEquals(0, table.getTuples().size());
        });
    }

    @Test
    public void testWithWhereNOTANDORClauses1() {
        // Reverses the predicates in the verifyTestWithWhereANDORClauses1 and adds NOT in front
        String queryStr = "MATCH (a:PERSON)-[e1:STUDYAT]->(b:ORGANISATION) " +
            "WITH b, sum(e1.year) as yearSum " +
            "WHERE NOT b.name <> \"CsWork\" AND NOT yearSum <= 5000 RETURN *";
        verifyTestWithWhereANDORClauses1(queryStr);
    }

    @Test
    public void testWithWhereANDORClauses2() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b, sum(e1.year) as yearSum " +
            "WHERE b.name = \"CsWork\" OR yearSum > 5000 RETURN b.orgCode, yearSum";
        verifyTestWithWhereANDORClauses2(queryStr);
    }

    private void verifyTestWithWhereANDORClauses2(String queryStr) {
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("b.orgCode", DataType.
                INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("yearSum", DataType.
                INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{934, 2015});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testWithWhereNOTANDORClauses2() {
        // Reverses the predicates in the verifyTestWithWhereANDORClauses2 and adds NOT in front
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b, sum(e1.year) as yearSum " +
            "WHERE NOT b.name <> \"CsWork\" OR NOT yearSum <= 5000 RETURN b.orgCode, yearSum";
        verifyTestWithWhereANDORClauses2(queryStr);
    }

    @Test
    public void testFilterOnSummationAggregatedValueTwoResults() {
        String queryStr = "MATCH (a:PERSON)-[e1:WORKAT]->(b:ORGANISATION) " +
            "WITH b.orgCode as code, sum(e1.year) as yearSum " +
            "WHERE yearSum > 2000 RETURN *";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("code", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("yearSum", DataType.
                INT));
            var tupleIntValues = new ArrayList<int[]>();
            tupleIntValues.add(new int[]{824, 2010+2015});
            tupleIntValues.add(new int[]{934, 2015});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testCountStarMultiPart() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a, e1, b " +
            "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON), (c:PERSON)-[e3:KNOWS]->(d:PERSON) " +
            "WHERE e2.date = e3.date RETURN count(*)";
        testCountStar(queryStr);
    }

    @Test
    public void testMultiProjection() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a, b " +
                "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN c.age, e2.date";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            System.out.println(plan);
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            // For the 4-clique, 4*3*3 outputs because there are that many 2-paths and the
            // projection does not remove row duplicates.
            // For 8<-7->9 part of the graph there are 4 outputs: 8, 8, 8, 9, 9, 8, and 9, 9
            // We check several outputs. There should be three 0,0's, two 2,5's, and one 8,8's
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("c.age", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("e2.date", DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            for (int i = 0; i < 3; ++i) {
                tupleIntValues.add(new int[]{35, 1234567890});
                tupleIntValues.add(new int[]{45, 1234567892});
                tupleIntValues.add(new int[]{20, 1234567892});
                tupleIntValues.add(new int[]{35, 1234567890});
                tupleIntValues.add(new int[]{30, 1234567892});
                tupleIntValues.add(new int[]{20, 1234567893});
                tupleIntValues.add(new int[]{35, 1234567890});
                tupleIntValues.add(new int[]{30, 1234567892});
                tupleIntValues.add(new int[]{45, 1234567893});
                tupleIntValues.add(new int[]{30, 1234567890});
                tupleIntValues.add(new int[]{45, 1234567890});
                tupleIntValues.add(new int[]{20, 1234567890});
            }
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testPropertyMultiProjection() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE e1.date >= 1234567892 " +
            " " +
            "WITH e1, b, a MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN  e1.date, " +
            "c.age";
//        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE e1.date >= 1234567892 " +
//            "RETURN a, b, e1";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
//        var plan = getAllPlansWithTupleStoringSink(queryStr).get(5);
            System.out.println(plan);
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            // total 3* (2*3) possibilities
            System.out.println(plan.getNumOutputTuples());
            var sampleTupleForTable = new Tuple();
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("c.age", DataType.INT));
            sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("e1.date", DataType.INT));
            var tupleIntValues = new ArrayList<int[]>();
            for (int i = 0; i < 3; ++i) {
                tupleIntValues.add(new int[]{45, 1234567892});
                tupleIntValues.add(new int[]{20, 1234567892});
            }
            for (int i = 0; i < 4; ++i) {
                tupleIntValues.add(new int[]{35, 1234567892});
            }
            for (int i = 0; i < 2; ++i) {
                tupleIntValues.add(new int[]{30, 1234567892});
                tupleIntValues.add(new int[]{35, 1234567893});
                tupleIntValues.add(new int[]{30, 1234567893});
            }
            tupleIntValues.add(new int[]{45, 1234567893});
            tupleIntValues.add(new int[]{20, 1234567893});
            var expectedTable = Table.constructTable(sampleTupleForTable, tupleIntValues);
//            table.printTable();
//            expectedTable.printTable();
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    @Test
    public void testRelMultiProjection() {
        String queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH *" +
                "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON), (c:PERSON)-[e3:KNOWS]->(d:PERSON) "+
                "WHERE e2.date = e3.date return a, b";
        getAllPlansWithTupleStoringSink(queryStr).forEach(plan -> {
            plan.init(graphTinySnb);
            plan.execute();
            var table = plan.getOutputTable();
            // there are 4 unique values of a vertexID {_:0, _:1, _:2, _:3} with counts :
            // for a = _:0 -> 17 = 5 + 6 + 6 ((b is _:2) plus (b is _:3) plus (b is _:5))
            // for a = _:2 -> 15 = 3 + 6 + 6 ((b is _:0) plus (b is _:3) plus (b is _:5))
            // for a = _:3 -> 14 = 3 + 5 + 6 ((b is _:0) plus (b is _:2) plus (b is _:5 or _:3))
            // for a = _:5 -> 14 = 3 + 5 + 6 ((b is _:0) plus (b is _:2) plus (b is _:5 or _:3))
            Assertions.assertEquals(17+15+14+14, table.getTuples().size());
            // The output table is very large, so we don't use the Table.isSame method. Instead,
            // we count that each of the a's appears for designated number of times in the table.
            // Since we don't know the vid deterministically, we group the table by a and assert
            // if the count is one of the values we expect.
            var nodeVals = table.getTuples().stream().map(tuple -> tuple.get(0)).collect(
                Collectors.toList());
            Map<String, Integer> uniqueNodeValCounts = new HashMap<>();
            nodeVals.forEach(value -> {
                var nums = uniqueNodeValCounts.getOrDefault(value.getValAsStr(), 0) + 1;
                uniqueNodeValCounts.put(value.getValAsStr(), nums);
            });
            var expectedCounts = new HashSet<Integer>(Arrays.asList(17, 15, 14));
            Assertions.assertEquals(4 /*number of unique a*/, uniqueNodeValCounts.size());
            uniqueNodeValCounts.forEach((key, value) -> {
                Assertions.assertTrue(expectedCounts.contains(value));
            });
        });
    }

    @Test
    public void testMinDateMultiPart() {
        String queryStr = "MATCH (a:PERSON)-[e2:KNOWS]->(c:PERSON) " +
            "WITH a " +
            "MATCH (a)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN b.age, min(e1.date)";
        testMinDate(queryStr);
    }

    @Test
    public void testSkipInMultiQuery() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH * " +
                "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN * SKIP 10", 4 * 3 * 3 - 10);
    }

    @Test
    public void testSkipNumberExceedTotalTuplesInMultiQuery() {
        testAllPlans("MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) WITH * " +
                "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN * SKIP 1000", 0);
    }

    @Test
    public void testLimitInMultiQuery() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH * " +
                "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN * LIMIT 5", 5);
    }

    @Test
    public void testLimitNumberExceedTotalTuplesInMultiQuery() {
        testAllPlans("MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) WITH * " +
                "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN * LIMIT 1000", 4 * 3 * 3);
    }

    @Test
    public void testSkipAndLimitInMultiQuery() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH * " +
                "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN * SKIP 10 LIMIT 5", 5);
    }

    @Test
    public void testSkipAndLimitExceedTotalTuplesInMultiQuery() {
        testAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH * " +
                "MATCH (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN * SKIP 10 LIMIT 1000",
                    4 * 3 * 3 - 10);
    }

    @Test
    public void testExceptionIfRelValNotProjectedWithNodeVals() {
        Assertions.assertThrows(MalformedQueryException.class, () -> {
            getAllPlans("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a " +
                "MATCH (a:PERSON)-[e1:KNOWS]->(c:PERSON) RETURN e1").forEach(plan -> {
                plan.init(graphTinySnb);
                plan.execute();
            });
        });
    }
}
