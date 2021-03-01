package ca.waterloo.dsg.graphflow.parser;

import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.ReturnBody;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.ReturnBody.ReturnBodyType;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getMockedGraphCatalog;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getNodeVariable;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getPropertyVariable;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getRelVariable;

public class ProjectionParserTests {

    @Test
    public void testSinglePartProjectionToNeverDeclaredSimpleVariable() {
        Assertions.assertThrows(MalformedQueryException.class, () -> {
            QueryParser.parseQuery("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN c",
                getMockedGraphCatalog());
        });
    }

    @Test
    public void testSinglePartProjectionToQueryNode() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN a",
            getMockedGraphCatalog()));
        var expectedReturnBody = new ReturnBody(ReturnBodyType.PROJECTION);
        expectedReturnBody.addNonAggregatingExpression(getNodeVariable("a"));
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.
            get(0).getReturnBody()));
    }

    @Test
    public void testSinglePartProjectionToQueryRel() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN a, b, e1",
            getMockedGraphCatalog()));
        var expectedReturnBody = new ReturnBody(ReturnBodyType.PROJECTION);
        var nodeVariableA = getNodeVariable("a");
        var nodeVariableB = getNodeVariable("b");
        expectedReturnBody.addNonAggregatingExpression(nodeVariableA);
        expectedReturnBody.addNonAggregatingExpression(nodeVariableB);
        expectedReturnBody.addNonAggregatingExpression(getRelVariable("e1", nodeVariableA,
            nodeVariableB));
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.
            get(0).getReturnBody()));
    }

    @Test
    public void testSinglePartProjectionToPropertyVariables() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN a.name, e1.date",
            getMockedGraphCatalog()));
        var expectedReturnBody = new ReturnBody(ReturnBodyType.PROJECTION);
        var nodeVariableA = getNodeVariable("a");
        var vertexPropertyVariable = getPropertyVariable("name", 2, DataType.INT, nodeVariableA);
        expectedReturnBody.addNonAggregatingExpression(vertexPropertyVariable);
        var edgePropertyVariable = getPropertyVariable("date", 2, DataType.INT, getRelVariable(
            "e1", nodeVariableA, getNodeVariable("b")));
        edgePropertyVariable.getNodeOrRelVariable().setDataType(DataType.RELATIONSHIP);
        expectedReturnBody.addNonAggregatingExpression(edgePropertyVariable);
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.
            get(0).getReturnBody()));
    }

    @Test
    public void testSinglePartProjectionToNeverDeclaredNodeOrRelPropertyVariable() {
        Assertions.assertThrows(MalformedQueryException.class, () ->
            QueryParser.parseQuery("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN c.name",
            getMockedGraphCatalog()));
    }

    @Test
    public void testMultiPartProjectionToNeverDeclaredVariable() {
        Assertions.assertThrows(MalformedQueryException.class, () ->
            QueryParser.parseQuery("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a " +
            "MATCH (a:PERSON)->(d:PERSON) RETURN a, c", getMockedGraphCatalog()));
    }

    @Test
    public void testMultiPartProjectionToOutofScopeVariable() {
        Assertions.assertThrows(MalformedQueryException.class, () ->
            QueryParser.parseQuery("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a " +
            "MATCH (a:PERSON)-[e2:KNOWS]->(d:PERSON) RETURN a, b", getMockedGraphCatalog()));
    }

    @Test
    public void testMultiPartWithStarProjectionReturnProjectionToQueryNode() {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH * " +
            "MATCH (a)-[e2:KNOWS]->(d:PERSON) RETURN d";
        var query = ((RegularQuery) QueryParser.parseQuery(queryStr,
            getMockedGraphCatalog()));

        var expectedReturnBody = new ReturnBody(ReturnBodyType.PROJECTION);
        expectedReturnBody.addNonAggregatingExpression(getNodeVariable("d"));
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.
            get(1).getReturnBody()));
    }

    @Test
    public void testMultiPartWithStarProjectionReturnProjectionToQueryRel() {
        var Queriestr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH * " +
            "MATCH (a)-[e2:KNOWS]->(d:PERSON) RETURN a, e2, d";
        var query = ((RegularQuery) QueryParser.parseQuery(Queriestr,
            getMockedGraphCatalog()));
        var expectedReturnBody = new ReturnBody(ReturnBodyType.RETURN_OR_WITH_STAR);
        var nodeVariableA = getNodeVariable("a");
        var nodeVariableB = getNodeVariable("b");
        expectedReturnBody.addNonAggregatingExpression(nodeVariableA);
        expectedReturnBody.addNonAggregatingExpression(nodeVariableB);
        expectedReturnBody.addNonAggregatingExpression(getRelVariable("e1", nodeVariableA,
            nodeVariableB));
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.
            get(0).getReturnBody()));
        expectedReturnBody = new ReturnBody(ReturnBodyType.PROJECTION);
        var nodeVariableD = getNodeVariable("d");
        expectedReturnBody.addNonAggregatingExpression(nodeVariableA);
        expectedReturnBody.addNonAggregatingExpression(nodeVariableD);
        expectedReturnBody.addNonAggregatingExpression(getRelVariable("e2", nodeVariableA,
            nodeVariableD));
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.
            get(1).getReturnBody()));
    }

    @Test
    public void testMultiPartProjectionWithProjectionToQueryNodeReturnProjectionToQueryRel() {
        var Queriestr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a " +
            "MATCH (a)-[e2:KNOWS]->(d:PERSON) RETURN a, d, e2";
        var query = ((RegularQuery) QueryParser.parseQuery(Queriestr,
            getMockedGraphCatalog()));
        var expectedReturnBody = new ReturnBody(ReturnBodyType.PROJECTION);
        var nodeVariableA = getNodeVariable("a");
        expectedReturnBody.addNonAggregatingExpression(nodeVariableA);
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.
            get(0).getReturnBody()));
        expectedReturnBody = new ReturnBody(ReturnBodyType.PROJECTION);
        var nodeVariableD = getNodeVariable("d");
        expectedReturnBody.addNonAggregatingExpression(nodeVariableA);
        expectedReturnBody.addNonAggregatingExpression(nodeVariableD);
        expectedReturnBody.addNonAggregatingExpression(getRelVariable("e2",nodeVariableA,
            nodeVariableD));
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.
            get(1).getReturnBody()));
    }

    @Test
    public void testMultiPartWithStarProjectionReturnProjectionToPropertyVariables() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH *" +
                "MATCH (a)-[e2:KNOWS]->(d:PERSON)" +
                "RETURN a.name, e2.date", getMockedGraphCatalog()));
        var expectedReturnBody = new ReturnBody(ReturnBodyType.RETURN_OR_WITH_STAR);
        var nodeVariableA = getNodeVariable("a");
        var nodeVariableB = getNodeVariable("b");
        expectedReturnBody.addNonAggregatingExpression(nodeVariableA);
        expectedReturnBody.addNonAggregatingExpression(nodeVariableB);
        expectedReturnBody.addNonAggregatingExpression(getRelVariable("e1", nodeVariableA,
            nodeVariableB));
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.
            get(0).getReturnBody()));
        expectedReturnBody = new ReturnBody(ReturnBodyType.PROJECTION);
        var vertexPropertyVariable = getPropertyVariable("name", 2, DataType.INT,
            getNodeVariable("a"));
        expectedReturnBody.addNonAggregatingExpression(vertexPropertyVariable);
        var edgePropertyVariable = getPropertyVariable("date", 2, DataType.INT, getRelVariable(
            "e2", nodeVariableA, getNodeVariable("d")));
        expectedReturnBody.addNonAggregatingExpression(edgePropertyVariable);
        Assertions.assertTrue(expectedReturnBody.isSame(query.singleQueries.get(0).queryParts.
            get(1).getReturnBody()));
    }
}