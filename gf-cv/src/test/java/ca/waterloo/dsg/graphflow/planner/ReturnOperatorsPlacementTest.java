package ca.waterloo.dsg.graphflow.planner;

import ca.waterloo.dsg.graphflow.parser.query.expressions.FunctionInvocation;
import ca.waterloo.dsg.graphflow.parser.query.expressions.SimpleVariable;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.AggregationFunction;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.groupbyaggregate.GroupByAggregate;
import ca.waterloo.dsg.graphflow.plan.operator.projection.PlaceHolderProjection;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.RelPropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.NodePropertyReader;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.GraphflowTestUtils;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getNodeVariable;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getPropertyVariable;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getRelVariable;

public class ReturnOperatorsPlacementTest extends AbstractEndToEndTests {

    @Test
    public void testProjectionPlacedSinglePartReturnStar()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN *";
        getAllPlans(queryStr).forEach(plan -> {
            var projection = plan.getLastOperator().getPrev();
            Assertions.assertTrue(projection instanceof PlaceHolderProjection);
            var expectedSchema = new Schema();
            var nodeVariableA = getNodeVariable("a");
            expectedSchema.add("a", nodeVariableA);
            var nodeVariableB = getNodeVariable("b");
            expectedSchema.add("b", nodeVariableB);
            expectedSchema.add("e1", getRelVariable("e1", nodeVariableA, nodeVariableB));
            Assertions.assertTrue(expectedSchema.isSame(projection.getOutSchema()));
        });
    }

    @Test
    public void testProjectionPlacedSinglePart1()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
                       "RETURN a";
        getAllPlans(queryStr).forEach(plan -> {
            // Last operator is Sink, so we skip over it.
            var projection = plan.getLastOperator().getPrev();
            Assertions.assertTrue(projection instanceof PlaceHolderProjection);
            var expectedSchema = new Schema();
            expectedSchema.add("a", getNodeVariable("a"));
            Assertions.assertTrue(expectedSchema.isSame(projection.getOutSchema()));
        });
    }

    @Test
    public void testProjectionAndVPropertyReaderPlacedSinglePart()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN a, b.gender";
        getAllPlans(queryStr).forEach(plan -> {
            // Last operator is Sink, so we skip over it.
            var projection = plan.getLastOperator().getPrev();
            Assertions.assertTrue(projection instanceof PlaceHolderProjection);
            var expectedSchema = new Schema();
            var nodeVariableA = getNodeVariable("a");
            expectedSchema.add("a", nodeVariableA);
            var nodeVariableB = getNodeVariable("b");
            var propertyVariable = getPropertyVariable("gender", 2, DataType.INT, nodeVariableB);
            expectedSchema.add("b.gender", propertyVariable);
            Assertions.assertTrue(expectedSchema.isSame(projection.getOutSchema()));
            var vPropertyReader = projection.getPrev();
            Assertions.assertTrue(vPropertyReader instanceof NodePropertyReader);
            expectedSchema = new Schema();
            expectedSchema.add("a", nodeVariableA);
            expectedSchema.add("b", nodeVariableB);
            expectedSchema.add("e1", getRelVariable("e1", nodeVariableA, nodeVariableB));
            expectedSchema.add("b.gender", propertyVariable);
            Assertions.assertTrue(expectedSchema.isSame(vPropertyReader.getOutSchema()));
        });
    }

    @Test
    public void testProjectionAndEPropertyReaderPlacedSinglePart() {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN a, e1.date";
        getAllPlans(queryStr).forEach(plan -> {
            // Last operator is Sink, so we skip over it.
            var projection = plan.getLastOperator().getPrev();
            Assertions.assertTrue(projection instanceof PlaceHolderProjection);
            var expectedSchema = new Schema();
            var nodeVariableA = getNodeVariable("a");
            expectedSchema.add("a", nodeVariableA);
            var nodeVariableB = getNodeVariable("b");
            var relVariable = getRelVariable("e1", nodeVariableA, nodeVariableB);
            var propertyVariable = getPropertyVariable("date", 2, DataType.INT, relVariable);
            expectedSchema.add("e1.date", propertyVariable);
            Assertions.assertTrue(expectedSchema.isSame(projection.getOutSchema()));
            var vPropertyReader = projection.getPrev();
            Assertions.assertTrue(vPropertyReader instanceof RelPropertyReader);
            expectedSchema = new Schema();
            expectedSchema.add("a", nodeVariableA);
            expectedSchema.add("b", nodeVariableB);
            expectedSchema.add("e1", relVariable);
            expectedSchema.add("e1.date", propertyVariable);
            Assertions.assertTrue(expectedSchema.isSame(vPropertyReader.getOutSchema()));
        });
    }

    @Test
    public void testProjectionAndBothVandEPropertyReaderPlacedSinglePart() {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN a, b.gender, e1.date";
        getAllPlans(queryStr).forEach(plan -> {
            // Last operator is Sink, so we skip over it.
            var projection = plan.getLastOperator().getPrev();
            Assertions.assertTrue(projection instanceof PlaceHolderProjection);
            var expectedSchema = new Schema();
            var nodeVariableA = getNodeVariable("a");
            expectedSchema.add("a", nodeVariableA);
            var nodeVariableB = getNodeVariable("b");
            var propertyVariableGender = getPropertyVariable("gender", 2, DataType.INT, nodeVariableB);
            expectedSchema.add("b.gender", propertyVariableGender);
            var relVariable = getRelVariable("e1", nodeVariableA, nodeVariableB);
            var propertyVariableDate = getPropertyVariable("date", 2, DataType.INT, relVariable);
            expectedSchema.add("e1.date", propertyVariableDate);
            Assertions.assertTrue(expectedSchema.isSame(projection.getOutSchema()));
            var propertyReader2 = projection.getPrev();
            var propertyReader1 = propertyReader2.getPrev();
            Assertions.assertTrue(propertyReader1 instanceof NodePropertyReader ||
                propertyReader2 instanceof NodePropertyReader);
            Assertions.assertTrue(propertyReader1 instanceof RelPropertyReader ||
                propertyReader2 instanceof RelPropertyReader);
            expectedSchema = new Schema();
            expectedSchema.add("a", nodeVariableA);
            expectedSchema.add("b", nodeVariableB);
            expectedSchema.add("e1", relVariable);
            if (propertyReader1 instanceof NodePropertyReader) {
                expectedSchema.add("b.gender", propertyVariableGender);
            } else {
                expectedSchema.add("e1.date", propertyVariableDate);
            }
            Assertions.assertTrue(expectedSchema.isSame(propertyReader1.getOutSchema()));

            if (propertyReader2 instanceof NodePropertyReader) {
                expectedSchema.add("b.gender", propertyVariableGender);
            } else {
                expectedSchema.add("e1.date", propertyVariableDate);
            }
            Assertions.assertTrue(expectedSchema.isSame(propertyReader2.getOutSchema()));
        });
    }

    @Test
    public void testProjectionPlacedSinglePart2()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN a, b, e1";
        getAllPlans(queryStr).forEach(plan -> {
            var projection = plan.getLastOperator().getPrev();
            Assertions.assertTrue(projection instanceof PlaceHolderProjection);
            var expectedSchema = new Schema();
            var nodeVariableA = getNodeVariable("a");
            var nodeVariableB = getNodeVariable("b");
            expectedSchema.add("a", nodeVariableA);
            expectedSchema.add("b", nodeVariableB);
            expectedSchema.add("e1", getRelVariable("e1", nodeVariableA, nodeVariableB));
            Assertions.assertTrue(expectedSchema.isSame(projection.getOutSchema()));
        });
    }

    @Test
    public void testProjectionPlacedMultiPart1()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WITH a, b " +
            "MATCH (b)-[e2:KNOWS]->(c:PERSON) " +
            "RETURN b, e2, c";
        getAllPlans(queryStr).forEach(plan -> {
            var projection2 = plan.getLastOperator().getPrev();
            assertProjectionsForTestProjectionPlacedMultiPart(projection2);
        });
    }

    private void assertProjectionsForTestProjectionPlacedMultiPart(Operator projection2) {
        Assertions.assertTrue(projection2 instanceof PlaceHolderProjection);
        var expectedSchema = new Schema();
        var nodeVariableB = getNodeVariable("b");
        var nodeVariableC = getNodeVariable("c");
        expectedSchema.add("b", nodeVariableB);
        expectedSchema.add("c", nodeVariableC);
        expectedSchema.add("e2", getRelVariable("e2", nodeVariableB, nodeVariableC));
        Assertions.assertTrue(expectedSchema.isSame(projection2.getOutSchema()));
        // the last projection should be preceded by an E/I which should be preceded by the
        // projection of the first part of the query.
        var projection1 = projection2.getPrev().getPrev();
        Assertions.assertTrue(projection1 instanceof PlaceHolderProjection);
        expectedSchema = new Schema();
        expectedSchema.add("a", getNodeVariable("a"));
        expectedSchema.add("b", nodeVariableB);
        Assertions.assertTrue(expectedSchema.isSame(projection1.getOutSchema()));
    }

    @Test
    public void testGroupByAggregatePlacedNoGroupByCountStarSinglePart()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN count(*)";
        assertNoGroupByCountStar(queryStr);
    }

    @Test
    public void testGroupByAggregatePlacedNoGroupByCountStarMultiPart()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WITH a, b " +
            "MATCH (b)-[e2:KNOWS]->(c:PERSON) " +
            "RETURN count(*)";
        assertNoGroupByCountStar(queryStr);
    }

    private void assertNoGroupByCountStar(String queryStr) {
        getAllPlans(queryStr).forEach(plan -> {
            // Last operator is Sink, so we skip over it.
            var groupByAggregate = plan.getLastOperator().getPrev();
            Assertions.assertTrue(groupByAggregate instanceof GroupByAggregate);
            var expectedSchema = new Schema();
            var countStarExpr = FunctionInvocation.newCountStar();
            expectedSchema.add(countStarExpr.getVariableName(), new SimpleVariable(
                countStarExpr.getVariableName(), countStarExpr.getDataType()));
            Assertions.assertTrue(expectedSchema.isSame(groupByAggregate.getOutSchema()));
        });
    }

    @Test
    public void testGroupByAggregatePlacedNoGroupByMaxRelPropertySinglePart()  {
        var queryStr = "MATCH (a:PERSON)-[ew:WORKAT]->(b:ORGANISATION) " +
            "RETURN max(ew.year)";
        assertNoGroupByMaxRelProperty(queryStr);
    }

    @Test
    public void testGroupByAggregatePlacedNoGroupByMaxRelPropertyMultiPart()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WITH a, b " +
            "MATCH (b)-[ew:WORKAT]->(c:ORGANISATION) " +
            "RETURN max(ew.year)";
        assertNoGroupByMaxRelProperty(queryStr);
    }

    private void assertNoGroupByMaxRelProperty(String queryStr) {
        getAllPlans(queryStr).forEach(plan -> {
            var groupByAggregate = plan.getLastOperator().getPrev();
            Assertions.assertTrue(groupByAggregate instanceof GroupByAggregate);
            var expectedSchema = new Schema();
            var nodeVariableB = getNodeVariable("b");
            var nodeVariableC = getNodeVariable("c");
            var relVariable = getRelVariable("ew", nodeVariableB, nodeVariableC);
            var propertyVariable = getPropertyVariable("year", 2, DataType.INT, relVariable);
            var expectedAggrVariable = new FunctionInvocation(AggregationFunction.MAX,
                propertyVariable);
            expectedSchema.add(expectedAggrVariable.getVariableName(), expectedAggrVariable);
            Assertions.assertTrue(expectedSchema.isSame(groupByAggregate.getOutSchema()));
        });
    }

    @Test
    public void testGroupByAggregatePlacedGroupByQueryNodeCountStarSinglePart()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN b, count(*)";
        assertGroupByQueryNodeCountStar(queryStr);
    }

    @Test
    public void testGroupByAggregatePlacedGroupByQueryNodeCountStarMultiPart()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WITH a, b " +
            "MATCH (b)-[e2:KNOWS]->(c:PERSON) " +
            "RETURN b, count(*)";
        assertGroupByQueryNodeCountStar(queryStr);
    }

    private void assertGroupByQueryNodeCountStar(String queryStr) {
        getAllPlans(queryStr).forEach(plan -> {
            // Last operator is Sink, so we skip over it.
            var groupByAggregate = plan.getLastOperator().getPrev();
            Assertions.assertTrue(groupByAggregate instanceof GroupByAggregate);
            var expectedSchema = new Schema();
            expectedSchema.add("b", getNodeVariable("b"));
            var countStarExpr = FunctionInvocation.newCountStar();
            expectedSchema.add(countStarExpr.getVariableName(), new SimpleVariable(
                countStarExpr.getVariableName(), countStarExpr.getDataType()));
            Assertions.assertTrue(expectedSchema.isSame(groupByAggregate.getOutSchema()));
        });
    }

    @Test
    public void testGroupByAggregatePlacedGroupByQNodeMinRelPropertySinglePart()  {
        var queryStr = "MATCH (a:PERSON)-[ek:KNOWS]->(b:PERSON) " +
            "RETURN b, min(ek.date)";
        assertGroupByQueryNodeMinRelProperty(queryStr);
    }

    @Test
    public void testGroupByAggregatePlacedGroupByQNodeMinRelPropertyMultiPart()  {
        var queryStr = "MATCH (a:PERSON)-[ek:KNOWS]->(b:PERSON) " +
            "WITH a, b " +
            "MATCH (b)-[ek:KNOWS]->(c:PERSON) " +
            "RETURN b, min(ek.date)";
        assertGroupByQueryNodeMinRelProperty(queryStr);
    }

    private void assertGroupByQueryNodeMinRelProperty(String queryStr) {
        getAllPlans(queryStr).forEach(plan -> {
            // Last operator is Sink, so we skip over it.
            var groupByAggregate = plan.getLastOperator().getPrev();
            Assertions.assertTrue(groupByAggregate instanceof GroupByAggregate);
            var expectedSchema = new Schema();
            var nodeVariableB = getNodeVariable("b");
            expectedSchema.add("b", nodeVariableB);
            var expectedAggrVariable = new FunctionInvocation(AggregationFunction.MIN,
                GraphflowTestUtils.getPropertyVariable("date", graphTinySnb.getGraphCatalog().
                    getRelPropertyKey("date"), DataType.INT, getRelVariable("ek",
                    getNodeVariable("a"), nodeVariableB)));
            expectedSchema.add(expectedAggrVariable.getVariableName(), new SimpleVariable(
                expectedAggrVariable.getVariableName(), expectedAggrVariable.getDataType()));
            Assertions.assertTrue(expectedSchema.isSame(groupByAggregate.getOutSchema()));
        });
    }

    @Test
    public void testGroupByAggregatePlacedGroupByQVPropertyCountStarSinglePart()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN b.gender, count(*)";
        assertGroupByQVPropertyGenderCountStar(queryStr);
    }

    @Test
    public void testGroupByAggregatePlacedGroupByQVPropertyCountStarMultiPart()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(c:PERSON) " +
            "WITH a " +
            "MATCH (a)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN b.gender, count(*)";
        assertGroupByQVPropertyGenderCountStar(queryStr);
    }

    private void assertGroupByQVPropertyGenderCountStar(String queryStr) {
        getAllPlans(queryStr).forEach(plan -> {
            // Last operator is Sink, so we skip over it.
            var groupByAggregate = plan.getLastOperator().getPrev();
            Assertions.assertTrue(groupByAggregate instanceof GroupByAggregate);
            var expectedSchema = new Schema();
            var nodeVariableB = getNodeVariable("b");
            var propertyVariableGender = getPropertyVariable("gender", 2, DataType.INT,
                nodeVariableB);
            expectedSchema.add("b.gender", propertyVariableGender);
            var countStarExpr = FunctionInvocation.newCountStar();
            expectedSchema.add(countStarExpr.getVariableName(), new SimpleVariable(
                countStarExpr.getVariableName(), countStarExpr.getDataType()));
            Assertions.assertTrue(expectedSchema.isSame(groupByAggregate.getOutSchema()));
            var propertyReader = groupByAggregate.getPrev();
            Assertions.assertTrue(propertyReader instanceof NodePropertyReader);
            expectedSchema = new Schema();
            var nodeVariableA = getNodeVariable("a");
            expectedSchema.add("a", nodeVariableA);
            expectedSchema.add("b", nodeVariableB);
            expectedSchema.add("e1", getRelVariable("e1", nodeVariableA, nodeVariableB));
            expectedSchema.add("b.gender", propertyVariableGender);
            Assertions.assertTrue(expectedSchema.isSame(propertyReader.getOutSchema()));
        });
    }

    @Test
    public void testGroupByAggregatePlacedGroupByQVPropertySumRelPropertySinglePart()  {
        var queryStr = "MATCH (a:PERSON)-[ew:WORKAT]->(b:ORGANISATION) " +
            "RETURN a.gender, sum(ew.year)";
        assertGroupByQVPropertyGenderSumRelProperty(queryStr);
    }

    @Test
    public void testGroupByAggregatePlacedGroupByQVPropertySumRelPropertyMultiPart()  {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(c:PERSON) " +
            "WITH a " +
            "MATCH (a:PERSON)-[ew:WORKAT]->(b:ORGANISATION) " +
            "RETURN a.gender, sum(ew.year)";
        assertGroupByQVPropertyGenderSumRelProperty(queryStr);
    }

        private void assertGroupByQVPropertyGenderSumRelProperty(String queryStr) {
        getAllPlans(queryStr).forEach(plan -> {
            // Last operator is Sink, so we skip over it.
            var groupByAggregate = plan.getLastOperator().getPrev();
            Assertions.assertTrue(groupByAggregate instanceof GroupByAggregate);
            var expectedSchema = new Schema();
            var nodeVariableA = getNodeVariable("a");
            var propertyVariableGender = getPropertyVariable("gender", 2, DataType.INT,
                nodeVariableA);
            expectedSchema.add("a.gender", propertyVariableGender);
            var nodeVariableB = getNodeVariable("b");
            var relVariable = getRelVariable("ew", nodeVariableA, nodeVariableB);
            var propertyVariableYear = getPropertyVariable("year", graphTinySnb.getGraphCatalog()
                .getRelPropertyKey("year"), DataType.INT, relVariable);
            var expectedAggrVariable = new FunctionInvocation(AggregationFunction.SUM,
                propertyVariableYear);
            expectedSchema.add(expectedAggrVariable.getVariableName(), expectedAggrVariable);
            Assertions.assertTrue(expectedSchema.isSame(groupByAggregate.getOutSchema()));
            var propertyReader = groupByAggregate.getPrev();
            Assertions.assertTrue(propertyReader instanceof NodePropertyReader);
            expectedSchema = new Schema();
            expectedSchema.add("a", nodeVariableA);
            expectedSchema.add("b", nodeVariableB);
            // Normally the ew should not be part of the schema of the last property reader. The
            // property reader should only have ew.year. But for now there is a known issue that
            // does not remove it from the schema.
            expectedSchema.add("ew", relVariable);
            expectedSchema.add("ew.year", propertyVariableYear);
            expectedSchema.add("a.gender", propertyVariableGender);
            Assertions.assertTrue(expectedSchema.isSame(propertyReader.getOutSchema()));
        });
    }
}
