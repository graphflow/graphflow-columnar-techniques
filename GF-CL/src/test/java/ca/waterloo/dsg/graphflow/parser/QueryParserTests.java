package ca.waterloo.dsg.graphflow.parser;

import ca.waterloo.dsg.graphflow.parser.query.OrderByConstraint;
import ca.waterloo.dsg.graphflow.parser.query.OrderByConstraint.OrderType;
import ca.waterloo.dsg.graphflow.parser.query.expressions.AliasExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.ArithmeticExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.BooleanConnectorExpression.ANDExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.BooleanConnectorExpression.ORExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.ComparisonExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.ExpressionUtils;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.WithWhere;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.GraphflowTestUtils;
import ca.waterloo.dsg.graphflow.util.datatype.ComparisonOperator;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getMockedGraphCatalog;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getNodeVariable;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getPropertyVariable;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getRelVariable;
import static org.mockito.ArgumentMatchers.anyInt;

public class QueryParserTests {

    @Test
    public void testQueryVariablesStartingWith_GFNotAllowed1() {
        Assertions.assertThrows(MalformedQueryException.class, () ->
            QueryParser.parseQuery("MATCH (_gfX:PERSON)->(b:PERSON) RETURN *",
            getMockedGraphCatalog()));
    }

    @Test
    public void testMissingQueryRelVariableIsAdded() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[:KNOWS]->(b:PERSON)" +
                " RETURN *",
            getMockedGraphCatalog()));
        Assertions.assertEquals("_gFE0", query.singleQueries.get(0).queryParts.get(0).
            getMatchGraphSchema().getRelVariables().iterator().next().getVariableName());
    }

    @Test
    public void testParallelRelsErrorInAQueryPart() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), (b:PERSON)-[e2:KNOWS]->(c:PERSON), " +
                "(a:PERSON)-[e3:KNOWS]->(b:PERSON) RETURN *",
            getMockedGraphCatalog()));
    }

    @Test
    public void testParallelRelsErrorAcrossQueryParts() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), " +
                "(b:PERSON)-[e2:KNOWS]->(c:PERSON) " +
                "WITH * " +
                "MATCH (c:PERSON)-[e3:KNOWS]->(d:PERSON), " +
                "(b:PERSON)-[e4:KNOWS]->(c:PERSON) RETURN *",
            getMockedGraphCatalog()));
    }

    @Test
    public void testDuplicateRelsErrorInAQueryPart() {
        Assertions.assertThrows(MalformedQueryException.class, () ->
            QueryParser.parseQuery("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), " +
                "(b:PERSON)-[e2:KNOWS]->(c:PERSON), (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN *",
            getMockedGraphCatalog()));
    }

    @Test
    public void testDuplicateRelsErrorAcrossQueryParts() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON), " +
                "(b:PERSON)-[e2:KNOWS]->(c:PERSON) " +
                "WITH * " +
                "MATCH (c:PERSON)-[e3:KNOWS]->(d:PERSON), " +
                "(b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN *",
            getMockedGraphCatalog()));
    }

    @Test
    public void testParsingRelMatch() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[:KNOWS]->(b:PERSON) RETURN *",
            getMockedGraphCatalog()));
        Assertions.assertEquals(query.singleQueries.get(0).queryParts.size(), 1);
        var matchGraph = query.singleQueries.get(0).queryParts.get(0).getMatchGraphSchema();
        Assertions.assertEquals(matchGraph.getRelVariables().size(), 1);
        var relVariable = matchGraph.getRelVariables().iterator().next();
        Assertions.assertEquals(relVariable.getSrcNode().getVariableName(), "a");
        Assertions.assertEquals(relVariable.getDstNode().getVariableName(), "b");
    }

    @Test
    public void testParsingTwoRelsMatch() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[:KNOWS]->(b:PERSON), (b:PERSON)-[e2:KNOWS]->(c:PERSON) RETURN *",
            getMockedGraphCatalog()));
        Assertions.assertEquals(query.singleQueries.get(0).queryParts.size(), 1);
        var matchGraph = query.singleQueries.get(0).queryParts.get(0).getMatchGraphSchema();
        var relVariables = new ArrayList<RelVariable>(matchGraph.getRelVariables());
        relVariables.sort(Comparator.comparing(Expression::getVariableName));
        Assertions.assertEquals(relVariables.size(), 2);
        Assertions.assertEquals(relVariables.get(0).getSrcNode().getVariableName(), "a");
        Assertions.assertEquals(relVariables.get(0).getDstNode().getVariableName(), "b");
        Assertions.assertEquals(relVariables.get(1).getSrcNode().getVariableName(), "b");
        Assertions.assertEquals(relVariables.get(1).getDstNode().getVariableName(), "c");
    }

    @Test
    public void testParsingWITHClauseBrokenTwoChain() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[:KNoWS]->(b:PERSON) WITH * " +
                "MATCH (b:PERSON)-[:KNOWS]->(c:PERSON) RETURN *",
            getMockedGraphCatalog()));
        verifyBrokenTwoChainQueryPartSizeAndQueryRels(query, "a", "b", "b", "c");
        Assertions.assertFalse(query.singleQueries.get(0).queryParts.get(0).
            getInputSchemaMatchWhere().hasWhereExpression());
    }

    @Test
    public void testParsingWITHClauseBrokenTwoChainWITHPredicates() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[:KNOWS]->(b:PERSON) WHERE a.age > 10 WITH * " +
                "MATCH (b:PERSON)-[:KNOWS]->(c:PERSON) RETURN *",
            getMockedGraphCatalog()));
        verifyBrokenTwoChainQueryPartSizeAndQueryRels(query, "a", "b", "b", "c");
        Assertions.assertEquals(ExpressionUtils.getComparisonExpressionsInConjunctiveParts(
            query.singleQueries.get(0).queryParts.get(0).getInputSchemaMatchWhere().
                getWhereExpression()).size(), 1);
        Assertions.assertFalse(query.singleQueries.get(0).queryParts.get(1).
            getInputSchemaMatchWhere().hasWhereExpression());
    }

    @Test
    public void testParsingWITHClauseCartesianProduct() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)->(b:PERSON) WITH * " +
                "MATCH (c:PERSON)->(d:PERSON) RETURN *", getMockedGraphCatalog()));
    }

    private void verifyBrokenTwoChainQueryPartSizeAndQueryRels(RegularQuery query,
        String firstQPartFromV, String firstQPartToV, String secondQPartFromV,
        String secondQPartToV) {
        Assertions.assertEquals(query.singleQueries.get(0).queryParts.size(), 2);
        verifyBrokenTwoChainQueryPart(query.singleQueries.get(0).queryParts.get(0).
            getMatchGraphSchema(), firstQPartFromV, firstQPartToV);
        verifyBrokenTwoChainQueryPart(query.singleQueries.get(0).queryParts.get(1).
            getMatchGraphSchema(), secondQPartFromV, secondQPartToV);
    }

    private void verifyBrokenTwoChainQueryPart(Schema matchGraphSchema, String fromV, String toV) {
        Assertions.assertEquals(matchGraphSchema.getNodeVariables().size(), 2);
        Assertions.assertTrue(matchGraphSchema.containsNodeVarName(fromV));
        Assertions.assertTrue(matchGraphSchema.containsNodeVarName(toV));
        Assertions.assertEquals(((NodeVariable) matchGraphSchema.getExpression(fromV)).getType(),
            0);
        Assertions.assertEquals(((NodeVariable) matchGraphSchema.getExpression(toV)).getType(), 0);
        Assertions.assertEquals(matchGraphSchema.getRelVariables().size(), 1);
        var relVariable = matchGraphSchema.getRelVariables().iterator().next();
        Assertions.assertEquals(relVariable.getSrcNode().getVariableName(), fromV);
        Assertions.assertEquals(relVariable.getDstNode().getVariableName(), toV);
        Assertions.assertEquals(relVariable.getLabel(), 1);
    }

    @Test
    public void testParsingWITHClause3WITHClausesWithLabelsAndPredicates() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (c:PERSON)-[e1:KNOWS]->(a:PERSON), (a:PERSON)-[:LIKES]->(d:PERSON) " +
                "WHERE d.age > 1 WITH * " +
                "MATCH (b:PERSON)-[:KNOWS]->(c:PERSON) WITH *" +
                "MATCH (a:PERSON)-[e2:EMPLOYS]->(e:PERSON) WHERE e2.date > 100 RETURN *",
            getMockedGraphCatalog()));

        // Below when checking edge labels, label 1 is the keyStore's mock edge label value returned
        // in Mocker.getKeyStoreMock(). Similarly type 0 is used for mock vertex types.
        Assertions.assertEquals(query.singleQueries.get(0).queryParts.size(), 3);
        var qPart1 = query.singleQueries.get(0).queryParts.get(0);
        var qPart1MatchGraphSchema = qPart1.getMatchGraphSchema();
        // Verify qPart1 edges
        Assertions.assertEquals(qPart1MatchGraphSchema.getRelVariables().size(), 2);
        var relVariablesList = new ArrayList<>(qPart1MatchGraphSchema.getRelVariables());
        relVariablesList.sort(Comparator.comparing(Expression::getVariableName));
        Assertions.assertEquals(relVariablesList.get(0).getSrcNode().getVariableName(), "a");
        Assertions.assertEquals(relVariablesList.get(0).getDstNode().getVariableName(), "d");
        Assertions.assertEquals(relVariablesList.get(0).getLabel(), 1);
        Assertions.assertEquals(relVariablesList.get(1).getSrcNode().getVariableName(), "c");
        Assertions.assertEquals(relVariablesList.get(1).getDstNode().getVariableName(), "a");
        Assertions.assertEquals(relVariablesList.get(1).getLabel(), 1);
        // Verify qPart1 vertices
        Assertions.assertEquals(qPart1MatchGraphSchema.getNodeVariables().size(), 3);
        Assertions.assertEquals(((NodeVariable) qPart1MatchGraphSchema.getExpression("c")).
            getType(), 0);
        Assertions.assertEquals(((NodeVariable) qPart1MatchGraphSchema.getExpression("a")).
            getType(), 0);
        Assertions.assertEquals(((NodeVariable) qPart1MatchGraphSchema.getExpression("d")).
            getType(), 0);
        // Verify qPart1 has a predicate
        Assertions.assertEquals(ExpressionUtils.getComparisonExpressionsInConjunctiveParts(
            qPart1.getInputSchemaMatchWhere().getWhereExpression()).size(), 1);

        var qPart2 = query.singleQueries.get(0).queryParts.get(1);
        var qPart2MatchGraphSchema = qPart2.getMatchGraphSchema();
        // Verify qPart2 edges
        Assertions.assertEquals(qPart2MatchGraphSchema.getRelVariables().size(), 1);
        var qPart2RelVariable1 = qPart2MatchGraphSchema.getRelVariables().iterator().next();
        Assertions.assertEquals(qPart2RelVariable1.getSrcNode().getVariableName(), "b");
        Assertions.assertEquals(qPart2RelVariable1.getDstNode().getVariableName(), "c");
        Assertions.assertEquals(qPart2RelVariable1.getLabel(), 1);
        // Verify qPart2 vertices
        Assertions.assertEquals(qPart2MatchGraphSchema.getNodeVariables().size(), 2);
        Assertions.assertEquals(((NodeVariable) qPart2MatchGraphSchema.getExpression("b")).
            getType(), 0);
        // c is 0 because its type must be overridden by the more specific type specified before
        Assertions.assertEquals(((NodeVariable) qPart2MatchGraphSchema.getExpression("c")).
            getType(), 0);
        // Verify qPart2 does not have a predicate
        Assertions.assertFalse(qPart2.getInputSchemaMatchWhere().hasWhereExpression());

        var qPart3 = query.singleQueries.get(0).queryParts.get(2);
        var qPart3MatchGraphSchema = qPart3.getMatchGraphSchema();
        // Verify qPart3 edges
        Assertions.assertEquals(qPart3MatchGraphSchema.getRelVariables().size(), 1);
        var qPart3RelVariable1 = qPart3MatchGraphSchema.getRelVariables().iterator().next();
        Assertions.assertEquals(qPart3RelVariable1.getSrcNode().getVariableName(), "a");
        Assertions.assertEquals(qPart3RelVariable1.getDstNode().getVariableName(), "e");
        Assertions.assertEquals(qPart3RelVariable1.getLabel(), 1);
        // Verify qPart3 vertices
        Assertions.assertEquals(qPart3MatchGraphSchema.getNodeVariables().size(), 2);
        // a is 0 because its type must be overridden by the more specific type specified before
        Assertions.assertEquals(((NodeVariable) qPart3MatchGraphSchema.getExpression("a")).
            getType(), 0);
        Assertions.assertEquals(((NodeVariable) qPart3MatchGraphSchema.getExpression("e")).
            getType(), 0);
        // Verify qPart3 does not have a predicate
        Assertions.assertEquals(ExpressionUtils.getComparisonExpressionsInConjunctiveParts(
            qPart3.getInputSchemaMatchWhere().getWhereExpression()).size(), 1);
    }

    @Test
    public void testNonAliasedAggregationInWithClauseErrors() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)->(b:PERSON) WITH a, sum(b.age) " +
                "MATCH (a:PERSON)->(d:PERSON) RETURN *", getMockedGraphCatalog()));
    }

    @Test
    public void testPredicateInWithClauseIsOnAggregatedVariableNoGroupBy() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (c:PERSON)-[e1:KNOWS]->(a:PERSON) " +
                "WITH c " +
                "WHERE c.age > 100 " +
                "RETURN *", getMockedGraphCatalog()));
    }

    @Test
    public void testPredicateInWithClauseIsOnAggregatedVariableGroup() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (c:PERSON)-[e1:KNOWS]->(a:PERSON) " +
                "WITH c, sum(e1.year) as yearSum " +
                "WHERE c.age > 100 " +
                "RETURN *", getMockedGraphCatalog()));
    }

    @Test
    public void testNestedAggregationsErrors() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (c:PERSON)-[e1:KNOWS]->(a:PERSON) " +
                "RETURN c, sum(max(a.age))", getMockedGraphCatalog()));
    }

    @Test
    public void testPredicateInWithClause() {
        testPredicateInWithClause("MATCH (a:PERSON)-[:KNOWS]->(b:PERSON) WITH a, sum(b.age) as " +
            "sumAge WHERE sumAge >= 100 RETURN *");
    }

    @Test
    public void testPredicateInWithClauseLiteralIsNormalized() {
        testPredicateInWithClause("MATCH (a:PERSON)-[:KNOWS]->(b:PERSON) WITH a, sum(b.age) as " +
            "sumAge WHERE 100 <= sumAge RETURN *");
    }

    private void testPredicateInWithClause(String strQuery) {
        var query = (RegularQuery) QueryParser.parseQuery(strQuery,
            getMockedGraphCatalog());
        var withWhereExpr =
            ((WithWhere) query.singleQueries.get(0).queryParts.get(0).getReturnOrWith())
                .getWhereExpression();
        var predicate = (ComparisonExpression) withWhereExpr;
        Assertions.assertEquals(predicate.getComparisonOperator(), ComparisonOperator.
            GREATER_THAN_OR_EQUAL);
        Assertions.assertEquals(predicate.getLeftExpression().getVariableName(), "sumAge");
        Assertions.assertEquals(predicate.getLiteralTerm().getLiteral(), 100);
    }

    @Test
    public void testBothPredicatesAreNotLiterals() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)->(b:PERSON) WHERE 100 <= 150 RETURN *", getMockedGraphCatalog()));
    }

    @Test
    public void testStartsWithInPredicate() {
        var query = (RegularQuery) QueryParser.parseQuery(
            "MATCH (c:PERSON)-[e1:KNOWS]->(a:PERSON) " +
                "WHERE c.name STARTS WITH \"abc\" RETURN *", GraphflowTestUtils.
                getMockedGraphCatalogReturningStringProperty());
        Assertions.assertEquals(ExpressionUtils.getComparisonExpressionsInConjunctiveParts(query.
            singleQueries.get(0).queryParts.get(0).getInputSchemaMatchWhere().getWhereExpression()).
                get(0).getComparisonOperator(), ComparisonOperator.STARTS_WITH);
    }

    @Test
    public void testEndsWithInPredicate() {
        var query = (RegularQuery) QueryParser.parseQuery(
            "MATCH (c:PERSON)-[e1:KNOWS]->(a:PERSON) " +
                "WHERE c.name ENDS WITH \"abc\" RETURN *", GraphflowTestUtils.
                getMockedGraphCatalogReturningStringProperty());
        Assertions.assertEquals(ExpressionUtils.getComparisonExpressionsInConjunctiveParts(query.
            singleQueries.get(0).queryParts.get(0).getInputSchemaMatchWhere().getWhereExpression()).
                get(0).getComparisonOperator(), ComparisonOperator.ENDS_WITH);
    }

    @Test
    public void testContainsInPredicate() {
        var query = (RegularQuery) QueryParser.parseQuery(
            "MATCH (c:PERSON)-[e1:KNOWS]->(a:PERSON) " +
                "WHERE c.name CONTAINS \"abc\" RETURN *", GraphflowTestUtils.
                getMockedGraphCatalogReturningStringProperty());
        Assertions.assertEquals(ExpressionUtils.getComparisonExpressionsInConjunctiveParts(query.
            singleQueries.get(0).queryParts.get(0).getInputSchemaMatchWhere().getWhereExpression()).
                get(0).getComparisonOperator(), ComparisonOperator.CONTAINS);
    }

    @Test
    public void testANDNonBooleanSubexpression() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WHERE a.gender*2 AND" +
                " b.gender = 2 RETURN *", getMockedGraphCatalog()));
    }

    @Test
    public void testORNonBooleanSubexpression() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
                "WHERE a.gender = 1 OR b.gender/2 RETURN *", getMockedGraphCatalog()));
    }

    @Test
    public void testANDBooleanProperty() {
        var catalog = getMockedGraphCatalog();
        Mockito.when(catalog.getNodePropertyDataType(anyInt())).thenReturn(DataType.BOOLEAN);
        var query = (RegularQuery) QueryParser.parseQuery(
            "MATCH (c:PERSON)-[e1:KNOWS]->(a:PERSON) " +
                "WHERE c.isMarried AND c.isWorker RETURN *", catalog);
        var whereExpression = query.singleQueries.get(0).queryParts.get(0)
            .getInputSchemaMatchWhere().getWhereExpression();
        Assertions.assertTrue(whereExpression instanceof ANDExpression);
    }

    @Test
    public void testORBooleanProperty() {
        var catalog = getMockedGraphCatalog();
        Mockito.when(catalog.getNodePropertyDataType(anyInt())).thenReturn(DataType.BOOLEAN);
        var query = (RegularQuery) QueryParser.parseQuery(
            "MATCH (c:PERSON)-[e1:KNOWS]->(a:PERSON) " +
                "WHERE c.isStudent OR c.isWorker RETURN *", catalog);
        var whereExpression = query.singleQueries.get(0).queryParts.get(0).
            getInputSchemaMatchWhere().getWhereExpression();
        Assertions.assertTrue(whereExpression instanceof ORExpression);
    }

    @Test
    public void testSkip() {
        var catalog = getMockedGraphCatalog();
        Mockito.when(catalog.getNodePropertyDataType(anyInt())).thenReturn(DataType.BOOLEAN);
        var query = (RegularQuery) QueryParser.parseQuery("MATCH (c:t1)-[e1:KNOWS]->(a:PERSON) " +
            "WHERE c.isStudent OR c.isWorker RETURN * SKIP 2", catalog);
        Assertions.assertEquals(query.singleQueries.get(0).queryParts.get(0).getReturnBody().
            getNumTuplesToSkip(), 2);
    }

    @Test
    public void testLimit() {
        var catalog = getMockedGraphCatalog();
        Mockito.when(catalog.getNodePropertyDataType(anyInt())).thenReturn(DataType.BOOLEAN);
        var query = (RegularQuery) QueryParser.parseQuery("MATCH (c:t1)-[e1:l1]->(a:t1) " +
            "WHERE c.isStudent OR c.isWorker RETURN * LIMIT 7", catalog);
        Assertions.assertEquals(query.singleQueries.get(0).queryParts.get(0).getReturnBody().
            getNumTuplesToLimit(), 7);
    }

    @Test
    public void testSinglePartOrderByVariableOutOfScope() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN a ORDER BY c",
            getMockedGraphCatalog()));
    }

    @Test
    public void testSinglePartOrderByQueryNode() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN a ORDER BY a",
            getMockedGraphCatalog()));
    }

    @Test
    public void testSinglePartOrderByQueryRel() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) RETURN e1 ORDER BY e1",
            getMockedGraphCatalog()));
    }

    @Test
    public void testSinglePartOrderByNodePropertyVariables() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN a.name ORDER BY a.name", getMockedGraphCatalog()));
        var expectedOrderByExpression = getPropertyVariable("name", 2, DataType.INT,
            getNodeVariable("a"));
        Assertions.assertEquals(query.singleQueries.get(0).queryParts.get(0).getReturnBody().
            getOrderByConstraints().size(), 1);
        Assertions.assertEquals(expectedOrderByExpression, query.singleQueries.get(0).queryParts.
            get(0).getReturnBody().getOrderByConstraints().get(0).getExpression());
    }

    @Test
    public void testSinglePartOrderByRelPropertyVariables() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN e1.date ORDER BY e1.date", getMockedGraphCatalog()));
        var expectedOrderByExpression = getPropertyVariable("date", 2, DataType.INT,
            getRelVariable("e1", getNodeVariable("a"), getNodeVariable("b")));
        expectedOrderByExpression.getNodeOrRelVariable().setDataType(DataType.RELATIONSHIP);
        Assertions.assertEquals(query.singleQueries.get(0).queryParts.get(0).getReturnBody().
            getOrderByConstraints().size(), 1);
        Assertions.assertEquals(expectedOrderByExpression, query.singleQueries.get(0).queryParts.
            get(0).getReturnBody().getOrderByConstraints().get(0).getExpression());
    }

    @Test
    public void testMultiPartOrderByQueryNode() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
                "WITH * " +
                "MATCH (a:PERSON)-[e2:KNOWS]->(d:PERSON) RETURN d ORDER BY d",
            getMockedGraphCatalog()));
    }

    @Test
    public void testMultiPartOrderByQueryRel() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
                "WITH * " +
                "MATCH (a:PERSON)-[e2:KNOWS]->(d:PERSON) RETURN e2 ORDER BY e2",
            getMockedGraphCatalog()));
    }

    @Test
    public void testMultiPartOrderByPropertyVariables() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH * " +
                "MATCH (a:PERSON)-[e2:KNOWS]->(d:PERSON) " +
                "RETURN a.name, e2.date ORDER BY a.name",
            getMockedGraphCatalog()));
        var expectedOrderByExpression = getPropertyVariable("name", 2, DataType.INT,
            getNodeVariable("a"));
        expectedOrderByExpression.getNodeOrRelVariable().setDataType(DataType.NODE);
        Assertions.assertEquals(query.singleQueries.get(0).queryParts.get(1).getReturnBody().
            getOrderByConstraints().size(), 1);
        Assertions.assertEquals(expectedOrderByExpression, query.singleQueries.get(0).queryParts.
            get(1).getReturnBody().getOrderByConstraints().get(0).getExpression());
    }

    @Test
    public void testMultiPartReturnStarOrderByExpression() {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) WITH a, a.name AS name " +
            "MATCH (a:PERSON)-[e2:KNOWS]->(d:PERSON) RETURN * ORDER BY name";
        var query = ((RegularQuery) QueryParser.parseQuery(queryStr,
            getMockedGraphCatalog()));
        var propertyVariable = getPropertyVariable("name", 2 /*propertyKey*/, DataType.INT,
            getNodeVariable("a"));
        var expectedOrderByExpression = new AliasExpression(propertyVariable, "name");
        expectedOrderByExpression.setDataType(DataType.INT);
        Assertions.assertEquals(query.singleQueries.get(0).queryParts.get(1).getReturnBody().
            getOrderByConstraints().size(), 1);
        Assertions.assertEquals(expectedOrderByExpression, query.singleQueries.get(0).queryParts.
            get(1).getReturnBody().getOrderByConstraints().get(0).getExpression());
    }

    @Test
    public void testMultiPartReturnStarOrderByExpressionOutOfScope() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
                "WITH a, a.name AS name " +
                "MATCH (a:PERSON)-[e2:KNOWS]->(d:PERSON) " +
                "RETURN * ORDER BY c",
            getMockedGraphCatalog()));
    }

    @Test
    public void testOrderByArithmeticExpression() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN a.time * e1.date ORDER BY a.time * e1.date", getMockedGraphCatalog()));
        var nodeVariableA = getNodeVariable("a");
        var leftExpression = getPropertyVariable("time", 2, DataType.INT, nodeVariableA);
        var rightExpression = getPropertyVariable("date", 2, DataType.INT, getRelVariable("e1",
            nodeVariableA, getNodeVariable("b")));
        // "a.time * e1.date"
        var expectedOrderByExpression = new ArithmeticExpression(
            ArithmeticExpression.ArithmeticOperator.MULTIPLY, leftExpression, rightExpression);
        expectedOrderByExpression.setDataType(DataType.INT);
        Assertions.assertEquals(query.singleQueries.get(0).queryParts.get(0).getReturnBody().
            getOrderByConstraints().size(), 1);
        Assertions.assertEquals(expectedOrderByExpression, query.singleQueries.get(0).queryParts.
            get(0).getReturnBody().getOrderByConstraints().get(0).getExpression());
    }

    @Test
    public void testSinglePartOrderByMultipleVariablesOutOfScope() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
                "RETURN a.time ORDER BY a.time, a.name",
            getMockedGraphCatalog()));
    }

    @Test
    public void testMultiPartOrderByMultipleVariablesOutOfScope() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
                "WITH * " +
                "MATCH (a:PERSON)-[e2:KNOWS]->(d:PERSON) " +
                "RETURN a.name ORDER BY b, e2.date",
            getMockedGraphCatalog()));
    }

    @Test
    public void testMultiPartOrderByMultiplePropertyVariables() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
                "WITH * " +
                "MATCH (a:PERSON)-[e2:KNOWS]->(d:PERSON) " +
                "RETURN a.name, e2.date ORDER BY a.name, e2.date",
            getMockedGraphCatalog()));
        var expectedOrderByExpressions = new ArrayList<>();
        var nodeVariableA = getNodeVariable("a");
        expectedOrderByExpressions.add(getPropertyVariable("name", 2, DataType.INT,
            nodeVariableA));
        expectedOrderByExpressions.add(getPropertyVariable("date", 2, DataType.INT, getRelVariable(
            "e2", nodeVariableA, getNodeVariable("d"))));
        checkOrderByMultipleVariablesExpressions(expectedOrderByExpressions, query.singleQueries.
            get(0).queryParts.get(1).getReturnBody().getOrderByConstraints(), 2);
    }

    @Test
    public void testMultiPartReturnStarOrderByMultipleExpressions() {
        var queryStr = "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "WITH a, a.name AS name, e1.date AS date " +
            "MATCH (a:PERSON)-[e2:KNOWS]->(d:PERSON) " +
            "RETURN * ORDER BY name, date";
        var query = ((RegularQuery) QueryParser.parseQuery(queryStr,
            getMockedGraphCatalog()));
        var expectedOrderByExpressions = new ArrayList<>();
        var nodeVariableA = getNodeVariable("a");
        var namePropertyVariable = getPropertyVariable("name", 2, DataType.INT, nodeVariableA);
        var expectedNameOrderBy = new AliasExpression(namePropertyVariable, "name");
        expectedNameOrderBy.setDataType(DataType.INT);
        var datePropertyVariable = getPropertyVariable("date", 2, DataType.INT, getRelVariable(
            "e1", nodeVariableA, getNodeVariable("b")));
        var expectedDateOrderBy = new AliasExpression(datePropertyVariable, "date");
        expectedDateOrderBy.setDataType(DataType.INT);
        expectedOrderByExpressions.add(expectedNameOrderBy);
        expectedOrderByExpressions.add(expectedDateOrderBy);
        checkOrderByMultipleVariablesExpressions(expectedOrderByExpressions, query.singleQueries.
            get(0).queryParts.get(1).getReturnBody().getOrderByConstraints(), 2);
    }

    @Test
    public void testMultiPartReturnStarOrderByMultipleExpressionOutOfScope() {
        Assertions.assertThrows(MalformedQueryException.class, () -> QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
                "WITH a, a.name AS name " +
                "MATCH (a:PERSON)-[e2:KNOWS]->(d:PERSON) " +
                "RETURN * ORDER BY b, e2.date",
            getMockedGraphCatalog()));
    }

    @Test
    public void testOrderByMultipleArithmeticExpression() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN a.time * e1.date, a.name ORDER BY a.time * e1.date, a.name",
            getMockedGraphCatalog()));
        var nodeVariableA = getNodeVariable("a");
        var leftExpression = getPropertyVariable("time", 2 /*propertyKey*/, DataType.INT,
            nodeVariableA);
        var rightExpression = getPropertyVariable("date", 2 /*propertyKey*/, DataType.INT,
            getRelVariable("e1", nodeVariableA, getNodeVariable("b")));
        var expectedOrderByArithmeticExpression = new ArithmeticExpression(
            ArithmeticExpression.ArithmeticOperator.MULTIPLY, leftExpression, rightExpression);
        expectedOrderByArithmeticExpression.setDataType(DataType.INT);
        var expectedOrderByExpressions = new ArrayList<>();
        expectedOrderByExpressions.add(expectedOrderByArithmeticExpression);
        expectedOrderByExpressions.add(getPropertyVariable("name", 2 /*propertyKey*/,
            DataType.INT, nodeVariableA));
        checkOrderByMultipleVariablesExpressions(expectedOrderByExpressions, query.singleQueries.
            get(0).queryParts.get(0).getReturnBody().getOrderByConstraints(), 2);
    }

    @Test
    public void testSinglePartOrderByNodePropertyVariableAsc() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN a.name ORDER BY a.name", getMockedGraphCatalog()));
        var expectedOrderByIsASCValues = new ArrayList<>();
        expectedOrderByIsASCValues.add(OrderType.ASCENDING);
        checkOrderByMultipleVariablesIsAsc(expectedOrderByIsASCValues, query.singleQueries.get(0).
            queryParts.get(0).getReturnBody().getOrderByConstraints(), 1);
    }

    @Test
    public void testSinglePartOrderByNodeAndRelPropertyVariablesDescAsc() {
        var query = ((RegularQuery) QueryParser.parseQuery(
            "MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON) " +
            "RETURN a.name, e1.date ORDER BY a.name DESC, e1.date ASC", getMockedGraphCatalog()));
        var expectedOrderByIsASCValues = new ArrayList<>();
        expectedOrderByIsASCValues.add(OrderType.DESCENDING);
        expectedOrderByIsASCValues.add(OrderType.ASCENDING);
        checkOrderByMultipleVariablesIsAsc(expectedOrderByIsASCValues, query.singleQueries.get(0).
            queryParts.get(0).getReturnBody().getOrderByConstraints(), 2);
    }

    private void checkOrderByMultipleVariablesExpressions (List expectedOrderByExpressions,
        List<OrderByConstraint> queryConstraints, int expectedSize) {
        Assertions.assertEquals(queryConstraints.size(), expectedSize);
        for (var i = 0; i < expectedSize; ++i) {
            Assertions.assertEquals(expectedOrderByExpressions.get(i), queryConstraints.get(i).
                getExpression());
        }
    }

    private void checkOrderByMultipleVariablesIsAsc(List expectedIsAscendings,
        List<OrderByConstraint> queryConstrains, int expectedSize) {
        Assertions.assertEquals(queryConstrains.size(), expectedSize);
        for (var i = 0; i < expectedSize; ++i) {
            Assertions.assertEquals(expectedIsAscendings.get(i), queryConstrains.get(i).
                getOrderType());
        }
    }
}