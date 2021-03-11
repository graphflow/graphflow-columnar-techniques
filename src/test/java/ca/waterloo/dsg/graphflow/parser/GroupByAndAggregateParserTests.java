package ca.waterloo.dsg.graphflow.parser;

import ca.waterloo.dsg.graphflow.parser.query.expressions.AliasExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.FunctionInvocation;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.AggregationFunction;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.ReturnBody;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.ReturnBody.ReturnBodyType;
import ca.waterloo.dsg.graphflow.util.GraphflowTestUtils;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getNodeVariable;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getPropertyVariable;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getRelVariable;

public class GroupByAndAggregateParserTests {

    @Test
    public void testSinglePartAggregationToNeverDeclaredSimpleVariable() {
        Assertions.assertThrows(MalformedQueryException.class, () -> {
            QueryParser.parseQuery("MATCH (a)-[e1:]->(b) RETURN a, sum(c.amount)",
                    GraphflowTestUtils.getMockedGraphCatalog());
        });
    }

    @Test
    public void testSinglePartNoGroupByAggregationCountStar() {
        testSinglePartNoGroupByAggregationCountStar(false);
    }

    @Test
    public void testSinglePartNoGroupByAggregationCountStarWithAlias() {
        testSinglePartNoGroupByAggregationCountStar(true);
    }

    private void testSinglePartNoGroupByAggregationCountStar(boolean withAlias) {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN count(*)" + (withAlias ? " AS foo" : ""),
            GraphflowTestUtils.getMockedGraphCatalog()));
        var expectedReturnBody = new ReturnBody(ReturnBodyType.GROUP_BY_AND_AGGREGATE);
        expectedReturnBody.addAggregatingExpression(withAlias ?
            new AliasExpression(FunctionInvocation.newCountStar(), "foo") :
            FunctionInvocation.newCountStar());
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.get(0).getReturnBody()));
    }

    @Test
    public void testSinglePartGroupByQNodeAggregationCountStar() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN a, count(*)",
            GraphflowTestUtils.getMockedGraphCatalog()));
        var expectedReturnBody = new ReturnBody(ReturnBodyType.GROUP_BY_AND_AGGREGATE);
        expectedReturnBody.addNonAggregatingExpression(getNodeVariable("a"));
        expectedReturnBody.addAggregatingExpression(FunctionInvocation.newCountStar());
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.get(0).getReturnBody()));
    }

    @Test
    public void testSinglePartGroupByQNodePropertyAggregationCountStar() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN a.age, count(*)",
            GraphflowTestUtils.getMockedGraphCatalog()));
        var expectedReturnBody = new ReturnBody(ReturnBodyType.GROUP_BY_AND_AGGREGATE);
        var propertyVariable = getPropertyVariable("age", 2, DataType.INT, getNodeVariable("a"));
        propertyVariable.getNodeOrRelVariable().setDataType(DataType.NODE);
        expectedReturnBody.addNonAggregatingExpression(propertyVariable);
        expectedReturnBody.addAggregatingExpression(FunctionInvocation.newCountStar());
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.get(0).getReturnBody()));
    }

    @Test
    public void testSinglePartGroupByQNodeAggregationQNodeProperty() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN a, sum(b.age)",
            GraphflowTestUtils.getMockedGraphCatalog()));
        var expectedReturnBody = new ReturnBody(ReturnBodyType.GROUP_BY_AND_AGGREGATE);
        expectedReturnBody.addNonAggregatingExpression(getNodeVariable("a"));
        var expectedAggregationVariable = new FunctionInvocation(AggregationFunction.SUM,
            getPropertyVariable("age", 2, DataType.INT, getNodeVariable("b")));
        expectedReturnBody.addAggregatingExpression(expectedAggregationVariable);
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.get(0).getReturnBody()));
    }

    @Test
    public void testSinglePartGroupByQNodeAggregationQRelProperty() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN b, max(e1.amount)",
            GraphflowTestUtils.getMockedGraphCatalog()));
        var expectedReturnBody = new ReturnBody(ReturnBodyType.GROUP_BY_AND_AGGREGATE);
        var nodeVariableB = getNodeVariable("b");
        expectedReturnBody.addNonAggregatingExpression(nodeVariableB);
        var expectedAggregationVariable = new FunctionInvocation(AggregationFunction.MAX,
            getPropertyVariable("amount", 2, DataType.INT, getRelVariable("e1",
                getNodeVariable("a"), nodeVariableB)));
        expectedReturnBody.addAggregatingExpression(expectedAggregationVariable);
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.get(0).getReturnBody()));
    }

    @Test
    public void testMultiPartAggregationToNeverDeclaredSimpleVariable() {
        Assertions.assertThrows(MalformedQueryException.class, () -> {
            QueryParser.parseQuery("MATCH (a)-[e1:]->(b) WITH a, b " +
                                   "MATCH (b)-[e2:]->(c) RETURN a, sum(d.amount)",
                GraphflowTestUtils.getMockedGraphCatalog());
        });
    }

    @Test
    public void testMultiPartAggregationInWithClause() {
        Assertions.assertThrows(MalformedQueryException.class, () -> {
            QueryParser.parseQuery(
                "MATCH (a)-[e1:]->(b) WITH b, sum(b.age) " +
                "MATCH (b)-[e2:]->(c) RETURN a, c",
                GraphflowTestUtils.getMockedGraphCatalog());
        });
    }

    @Test
    public void testMultiPartNoGroupByAggregationCountStar() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a " +
            "MATCH (a)-[e2:KNOWS]->(c:PERSON) RETURN count(*)",
            GraphflowTestUtils.getMockedGraphCatalog()));
        var expectedReturnBody1 = new ReturnBody(ReturnBodyType.PROJECTION);
        expectedReturnBody1.addNonAggregatingExpression(getNodeVariable("a"));
        Assertions.assertTrue(expectedReturnBody1.isSame(query.singleQueries.get(0).queryParts.
            get(0).getReturnBody()));
        var expectedReturnBody2 = new ReturnBody(ReturnBodyType.GROUP_BY_AND_AGGREGATE);
        expectedReturnBody2.addAggregatingExpression(FunctionInvocation.newCountStar());
        Assertions.assertTrue(expectedReturnBody2.isSame(query.singleQueries.get(0).queryParts.
            get(1).getReturnBody()));
    }

    @Test
    public void testMultiPartGroupByQNodeAggregationCountStar() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a " +
            "MATCH (a)-[e2:KNOWS]->(c:PERSON) RETURN c, count(*)",
            GraphflowTestUtils.getMockedGraphCatalog()));
        var expectedReturnBody1 = new ReturnBody(ReturnBodyType.PROJECTION);
        expectedReturnBody1.addNonAggregatingExpression(getNodeVariable("a"));
        Assertions.assertTrue(expectedReturnBody1.isSame(query.singleQueries.get(0).queryParts.
            get(0).getReturnBody()));
        var expectedReturnBody2 = new ReturnBody(ReturnBodyType.GROUP_BY_AND_AGGREGATE);
        expectedReturnBody2.addNonAggregatingExpression(getNodeVariable("c"));
        expectedReturnBody2.addAggregatingExpression(FunctionInvocation.newCountStar());
        Assertions.assertTrue(expectedReturnBody2.isSame(query.singleQueries.get(0).queryParts.get(1).getReturnBody()));
    }

    @Test
    public void testMultiPartGroupByQNodeAggregationQNodeProperty() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a " +
            "MATCH (a)-[e2:KNOWS]->(c:PERSON) RETURN a, sum(c.age)",
            GraphflowTestUtils.getMockedGraphCatalog()));
        var expectedReturnBody1 = new ReturnBody(ReturnBodyType.PROJECTION);
        var nodeVariableA = getNodeVariable("a");
        expectedReturnBody1.addNonAggregatingExpression(nodeVariableA);
        Assertions.assertTrue(expectedReturnBody1.isSame(query.singleQueries.get(0).queryParts.get(0).getReturnBody()));
        var expectedReturnBody2 = new ReturnBody(ReturnBodyType.GROUP_BY_AND_AGGREGATE);
        expectedReturnBody2.addNonAggregatingExpression(nodeVariableA);
        var expectedAggregationVariable = new FunctionInvocation(AggregationFunction.SUM,
            getPropertyVariable("age", 2, DataType.INT, getNodeVariable("c")));
        expectedReturnBody2.addAggregatingExpression(expectedAggregationVariable);
        Assertions.assertTrue(expectedReturnBody2.isSame(query.singleQueries.get(0).queryParts.get(1).getReturnBody()));
    }

    @Test
    public void testMultiPartGroupByQNodeAggregationQRelProperty() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a " +
            "MATCH (a)-[e2:KNOWS]->(c:PERSON) RETURN a, max(e2.amount)",
            GraphflowTestUtils.getMockedGraphCatalog()));
        var expectedReturnBody1 = new ReturnBody(ReturnBodyType.PROJECTION);
        var nodeVariableA = getNodeVariable("a");
        expectedReturnBody1.addNonAggregatingExpression(nodeVariableA);
        Assertions.assertTrue(expectedReturnBody1.isSame(query.singleQueries.get(0).queryParts.get(0).getReturnBody()));
        var expectedReturnBody2 = new ReturnBody(ReturnBodyType.GROUP_BY_AND_AGGREGATE);
        expectedReturnBody2.addNonAggregatingExpression(nodeVariableA);
        var expectedAggregationVariable = new FunctionInvocation(AggregationFunction.MAX,
            getPropertyVariable("amount", 2, DataType.INT, getRelVariable("e2", nodeVariableA,
                getNodeVariable("c"))));
        expectedReturnBody2.addAggregatingExpression(expectedAggregationVariable);
        Assertions.assertTrue(expectedReturnBody2.isSame(query.singleQueries.get(0).queryParts.get(1).getReturnBody()));
    }
}
