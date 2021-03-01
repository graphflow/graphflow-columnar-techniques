package ca.waterloo.dsg.graphflow.parser;

import ca.waterloo.dsg.graphflow.parser.query.OrderByConstraint;
import ca.waterloo.dsg.graphflow.parser.query.expressions.AliasExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.SimpleVariable;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.PlainReturnOrWith.PlainReturnBody;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.PlainReturnOrWith.PlainWith;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.Return;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.ReturnBody;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.ReturnBody.ReturnBodyType;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.WithWhere;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.MatchGraphSchemaUtils;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.PlainQueryPart;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.PlainSingleQuery;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.QueryPart;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.SingleQuery;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Implements rewrite and validation rules on parsed queries.
 *
 * TODO(Semih): We should implement query part merging. If a querypart B comes after a query part A
 * that does not do any aggregations, then the query parts can be merged. This is implemented but
 * currently removed.
 */
public class ValidatorAndSingleQueryConverter {

    public static SingleQuery validateAndRewrite(PlainSingleQuery plainQuery, GraphCatalog catalog) {
        SingleQuery singleQuery = new SingleQuery();
        var relVariables = new HashMap<String, RelVariable>();
        for (int i = 0; i < plainQuery.plainQueryParts.size(); ++i) {
            var plainQPart = plainQuery.plainQueryParts.get(i);
            var currentQPart = (i == 0) ? new QueryPart(new Schema()) : new QueryPart(
                singleQuery.queryParts.get(singleQuery.queryParts.size()-1).getOutputSchema());
            plainQPart.getRelVariables().forEach(relVariable -> validateAndInsertRelVariable(
                currentQPart, relVariable, catalog)
            );
            if (!currentQPart.getMatchGraphSchema().isEmpty()) {
                validateMatchGraphIsConnectedInternallyAndToPreviousPart(currentQPart,
                    i > 0);
            }
            if (plainQPart.hasWhereExpression()) {
                currentQPart.getInputSchemaMatchWhere().setWhereExpression(plainQPart.getWhereExpression());
                var whereExpression = currentQPart.getInputSchemaMatchWhere().getWhereExpression();
                whereExpression.verifyVariablesAndNormalize(currentQPart.getInputSchema(),
                    currentQPart.getMatchGraphSchema(), catalog);
            }
            setReturnBody(plainQPart, currentQPart, (i == plainQuery.plainQueryParts.size()-1),
                catalog);
            currentQPart.getMatchGraphSchema().getRelVariables().forEach(
                relVariable -> relVariables.put(relVariable.getVariableName(), relVariable));
            checkRelsAreReturnedWithDependantVertices(relVariables, currentQPart.
                getOutputSchema().getVariablesInLexOrder());
            singleQuery.queryParts.add(currentQPart);
        }
        return singleQuery;
    }

    private static void validateMatchGraphIsConnectedInternallyAndToPreviousPart(
        QueryPart currentQPart, boolean checkConnectionToPreviousPart) {
        if (!MatchGraphSchemaUtils.isConnected(currentQPart.getMatchGraphSchema())) {
            throw new MalformedQueryException("Match graphs in query parts need to be " +
                "connected. Otherwise a Cartesian product operation is needed and we do not" +
                "currently support Cartesian products.");
        }
        if (checkConnectionToPreviousPart) {
            for (var nodeVariable : currentQPart.getMatchGraphSchema().getNodeVariables()) {
                if (currentQPart.getInputSchema().containsNodeVarName(nodeVariable.getVariableName())) {
                    return;
                }
            }
            throw new MalformedQueryException("Match graph of a query part is not connected to " +
                "the previous query part. Match graphs have to be connected to previous " +
                "parts. Otherwise a Cartesian product operation is needed and we do not" +
                "currently support Cartesian products.");
        }
    }

    private static void setReturnBody(PlainQueryPart plainQPart, QueryPart currentQPart,
        boolean lastQueryPart, GraphCatalog catalog) {
        ReturnBody returnBody;
        PlainReturnBody plainReturnBody = plainQPart.getPlainReturnBody();
        checkNoDuplicateAliases(plainQPart.getPlainReturnBody());
        verifyAndUpdateExpressionsInReturnBody(plainQPart.getPlainReturnBody(),
            currentQPart, catalog);
        if (plainReturnBody.isReturnStar()) {
            returnBody = constructReturnStarClause(currentQPart);
        } else {
            if (!lastQueryPart) {
                checkNonSimpleVariableExpressionsAreAliased(plainReturnBody);
            }
            if (plainReturnBody.containsAggregatingExpression()) {
                returnBody = constructGroupByAggregateReturnClause(plainQPart.getPlainReturnBody());
            } else {
                returnBody = constructProjectionReturnClause(plainQPart.getPlainReturnBody());
            }
        }
        if (!plainReturnBody.getOrderByConstraints().isEmpty()) {
            var validatedOrderByConstraints = checkOrderByExpressionsAreInScopeAndSetExpression(
                plainReturnBody.getOrderByConstraints(), returnBody);
            returnBody.setOrderByConstraints(validatedOrderByConstraints);
        }
        if (-1 != plainReturnBody.getNumTuplesToSkip()) {
            returnBody.setNumTuplesToSkip(plainReturnBody.getNumTuplesToSkip());
        }
        if (-1 != plainReturnBody.getNumTuplesToLimit()) {
            returnBody.setNumTuplesToLimit(plainReturnBody.getNumTuplesToLimit());
        }
        currentQPart.setReturnOrWith(lastQueryPart ? new Return(returnBody) :
            new WithWhere(returnBody));
        if (!lastQueryPart) {
            if (((PlainWith) plainQPart.getPlainReturnOrWith()).hasWhereExpression()) {
                validateSetComparisonTypeAndInsertPredicateToWithClause(currentQPart,
                    ((PlainWith) plainQPart.getPlainReturnOrWith()).getWhereExpression(), catalog);
            }
        }
    }

    private static void checkNonSimpleVariableExpressionsAreAliased(PlainReturnBody plainReturnBody) {
        for (var expression : plainReturnBody.getExpressions()) {
            if (!(expression instanceof SimpleVariable) && !(expression instanceof AliasExpression)) {
                throw new MalformedQueryException("Non-simple expressions must be aliased in " +
                    "the WITH clause. expression type: " + expression.getClass().getSimpleName() +
                    " expressionName: " + expression.getVariableName() +
                    " printableExpression: " + expression.getPrintableExpression());
            }
        }
    }

    private static void checkNoDuplicateAliases(PlainReturnBody plainReturnBody) {
        var aliases = new HashSet<String>();
        plainReturnBody.getExpressions().forEach(expression -> {
            if (expression instanceof AliasExpression) {
                if (aliases.contains(expression.getVariableName())) {
                    throw new MalformedQueryException("Alias: " + expression.getVariableName() +
                        " is defined twice in the return or with clause.");
                } else {
                    aliases.add(expression.getVariableName());
                }
            }
        });
    }

    private static void verifyAndUpdateExpressionsInReturnBody(
        PlainReturnBody plainReturnBody, QueryPart currentQPart, GraphCatalog catalog) {
        var expressions = plainReturnBody.getExpressions();
        for (var i = 0; i < expressions.size(); i++) {
            var expr = expressions.get(i);
            expr.verifyVariablesAndNormalize(
                currentQPart.getInputSchema(), currentQPart.getMatchGraphSchema(), catalog);
            if (expr instanceof SimpleVariable &&
                (DataType.NODE == expr.getDataType() || DataType.RELATIONSHIP == expr.getDataType())) {
                expressions.set(i, currentQPart.getInputSchema().containsVarName(expr.getVariableName()) ?
                    currentQPart.getInputSchema().getExpression(expr.getVariableName()) :
                    currentQPart.getMatchGraphSchema().getExpression(expr.getVariableName()));
            }
        }
    }

    private static void checkRelsAreReturnedWithDependantVertices(
        Map<String, RelVariable> relVariables, Set<Entry<String, Expression>> outSchemaVariables) {
        for (var variable : outSchemaVariables) {
            if (DataType.RELATIONSHIP == variable.getValue().getDataType()) {
                var relVariable = relVariables.get(variable.getKey());
                var srcNodeCount = outSchemaVariables.stream().filter(
                    entry -> entry.getKey().equals(relVariable.getSrcNode().getVariableName())).count();
                var toVertexCount = outSchemaVariables.stream().filter(
                    entry -> entry.getKey().equals(relVariable.getDstNode().getVariableName())).count();
                if (1 != srcNodeCount|| 1 != toVertexCount) {
                    throw new MalformedQueryException("QueryRel: " + relVariable.getVariableName() +
                        " is not projected with it's source and destination vertices (" +
                        relVariable.getSrcNode().getVariableName() + ", " +
                        relVariable.getDstNode().getVariableName() + ").");
                }
            }
        }
    }

    /**
     * Validate each OrderBy expression E by finding E' in the return body
     * that has exactly the same variableName()
     * Replace E with E' because E''s data type is resolved
     */
    private static List<OrderByConstraint> checkOrderByExpressionsAreInScopeAndSetExpression(
        List<OrderByConstraint> unvalidatedOrderByConstraints, ReturnBody returnBody) {
        HashMap<String, Expression>  returnBodyExpressionsMap;
        List<OrderByConstraint> validatedOrderByConstraints = new ArrayList<>();
        if (ReturnBodyType.GROUP_BY_AND_AGGREGATE == returnBody.getReturnBodyType()) {
            returnBodyExpressionsMap = getHashMapOfReturnBodyExpressions(
                    returnBody.getGroupByAndAggregateExpressions().getExpressions());
        } else {
            returnBodyExpressionsMap = getHashMapOfReturnBodyExpressions(
                returnBody.getProjectionExpressions().getProjectionExpressions());
        }
        for (var unvalidatedOrderByConstraint : unvalidatedOrderByConstraints) {
            var equivalentExprInReturnBody = returnBodyExpressionsMap.get(
                unvalidatedOrderByConstraint.getExpression().getVariableName());
            if (null == equivalentExprInReturnBody) {
                throw new MalformedQueryException("Order by : "
                    + unvalidatedOrderByConstraint.getExpression().getVariableName()
                    + " is not in projection expressions in return body.");
            }
            if (!isOrderByExpressionDataTypeSupported(equivalentExprInReturnBody)) {
                throw new MalformedQueryException("Order by data type: " +
                    equivalentExprInReturnBody.getDataType() + "is not supported.");
            }
            validatedOrderByConstraints.add(new OrderByConstraint(
                equivalentExprInReturnBody, unvalidatedOrderByConstraint.getOrderType()));
        }
        return validatedOrderByConstraints;
    }

    private static boolean isOrderByExpressionDataTypeSupported(Expression expression) {
        if (expression.getDataType() == DataType.RELATIONSHIP ||
            expression.getDataType() == DataType.UNKNOWN) {
            return false;
        }
        return true;
    }
    private static HashMap<String, Expression> getHashMapOfReturnBodyExpressions(
        List<Expression> returnBodyExpressions) {
        HashMap<String, Expression> returnBodyExpressionsHashMap = new HashMap<>();
        for (var expression : returnBodyExpressions) {
            returnBodyExpressionsHashMap.put(expression.getVariableName(), expression);
        }
        return returnBodyExpressionsHashMap;
    }

    private static ReturnBody constructGroupByAggregateReturnClause(PlainReturnBody plainReturnBody) {
        var retVal = new ReturnBody(ReturnBodyType.GROUP_BY_AND_AGGREGATE);
        plainReturnBody.getAggregatingExpressions().forEach(expr -> {
            if (expr.getDependentFunctionInvocations().size() > 1) {
                throw new MalformedQueryException("You can not have nested aggregations." +
                    " The query contains nested aggregation.");
            }
            retVal.addAggregatingExpression(expr);
        });
        plainReturnBody.getNonAggregatingExpressions().forEach(retVal::addNonAggregatingExpression);
        return retVal;
    }

    private static ReturnBody constructProjectionReturnClause(PlainReturnBody plainReturnBody) {
        var retVal = new ReturnBody(ReturnBodyType.PROJECTION);
        plainReturnBody.getExpressions().forEach(retVal::addNonAggregatingExpression);
        return retVal;
    }

    private static ReturnBody constructReturnStarClause(QueryPart currentQPart) {
        var retVal = new ReturnBody(ReturnBodyType.RETURN_OR_WITH_STAR);
        currentQPart.getInputSchema().getVariablesInLexOrder().forEach(varNameExpr ->
            retVal.addNonAggregatingExpression(varNameExpr.getValue()));
        currentQPart.getMatchGraphSchema().getVariablesInLexOrder().forEach(varNameExpr -> {
            var varName = varNameExpr.getKey();
            if (!varName.toLowerCase().startsWith(ParseTreeVisitor._gFUncapitalized)) {
                retVal.addNonAggregatingExpression(varNameExpr.getValue());
            }
        });
        return retVal;
    }

    private static void validateSetComparisonTypeAndInsertPredicateToWithClause(
        QueryPart currentQPart, Expression whereExpr, GraphCatalog catalog) {
        var with = (WithWhere) currentQPart.getReturnOrWith();
        with.setWhereExpression(whereExpr);
        Schema schema = currentQPart.getOutputSchema().copy();
        if (null == with.getReturnBody().getGroupByAndAggregateExpressions()) {
            throw new MalformedQueryException("WHERE expressions in the WITH clause have to " +
                "contain at least one sub-expression on an aggregated variable. This WITH clause " +
                "does not have a group by and aggregation at all.");
        }
        var aggregationVariables =
            with.getReturnBody().getGroupByAndAggregateExpressions().getAggregationExpressions();
        if (whereExprDependsOnAggregatedVariable(whereExpr, aggregationVariables)) {
            throw new MalformedQueryException("WHERE expressions in the WITH clause have to " +
                "contain at least one sub-expression on an aggregated variable. The WHERE " +
                "expression: " + whereExpr.getPrintableExpression() + " does not depend on " +
                "an aggregated variable.");
        }
        whereExpr.verifyVariablesAndNormalize(schema,
            currentQPart.getMatchGraphSchema(), catalog);
    }

    private static boolean whereExprDependsOnAggregatedVariable(Expression whereExpr,
        List<Expression> aggregationVariables) {
        for (Expression aggregationVariable : aggregationVariables) {
            if (whereExpr.getDependentVariableNames().contains(aggregationVariable.getVariableName())) {
                return false;
            }
        }
        return true;
    }

    private static void validateAndInsertRelVariable(QueryPart queryPart, RelVariable relVariable,
        GraphCatalog catalog) {
        validateNoSelfLoop(relVariable.getSrcNode(), relVariable.getDstNode());
        validateRelVarIsNotAlreadyMatched(queryPart, relVariable);
        validateNodeVariablesHaveDataTypeNodeIfAlreadyMatched(queryPart, relVariable);
        relVariable.verifyVariablesAndNormalize(queryPart.getInputSchema(),
            queryPart.getMatchGraphSchema(), catalog);
        validateRelVaraibleAgainstGraphCatalog(relVariable, catalog);
        validateBothQueryVerticesAreNotAlreadyMatched(queryPart.getInputSchema(), relVariable);
        validateNoParallelRels(queryPart.getMatchGraphSchema(), relVariable);
        queryPart.getMatchGraphSchema().addRelVariable(relVariable);
    }

    private static void validateNoSelfLoop(NodeVariable srcNode, NodeVariable dstNode) {
        if (srcNode.getVariableName().equals(dstNode.getVariableName())) {
            throw new MalformedQueryException("Detected a self loop for query vertex (" +
                srcNode.getVariableName() + "). Self loop edge is not supported.");
        }
    }

    private static void validateRelVaraibleAgainstGraphCatalog(RelVariable relVariable,
        GraphCatalog catalog) {
        if (!catalog.typeLabelExistsForDirection(relVariable.getSrcNode().getType(),
            relVariable.getLabel(), Direction.FORWARD)) {
            throw new MalformedQueryException("Source Node Type: " +
                relVariable.getSrcNode().getType() + " does not have forward edges having " +
                "label: " + relVariable.getLabel());
        }
        if (!catalog.typeLabelExistsForDirection(relVariable.getDstNode().getType(),
            relVariable.getLabel(), Direction.BACKWARD)) {
            throw new MalformedQueryException("Destination Node Type: " +
                relVariable.getDstNode().getType() + " does not have backward edges having " +
                "label: " + relVariable.getLabel());
        }
    }

    // This validation is different than parallel edges check. This merely checks that if we have
    // a query edge (a)->(b) in a query part, then both a and b cannot have already been matched in
    // a previous query part. Normally we should allow this and this does not necessarily mean that
    // the addition of this query edge will create a parallel edge between a and b. Because a and
    // b may have been matched through 2 or longer length of edges. We are doing this as a
    // precaution that in case the query parts do not get merged, we don't currently have an
    // operator to match additional edges between two already matched vertices. When we add that
    // feature, we should removeVariable this check and also allow parallel edges in general.
    private static void validateBothQueryVerticesAreNotAlreadyMatched(Schema inputSchema,
        RelVariable relVariable) {
        if (inputSchema.containsNodeVarName(relVariable.getSrcNode().getVariableName()) &&
            inputSchema.containsNodeVarName(relVariable.getDstNode().getVariableName())) {
            throw new MalformedQueryException("Trying to match an edge between two already " +
                "matched vertices in a previous query part. Currently this is not allowed. " +
                "Query vertices: (" + relVariable.getSrcNode().getVariableName() + ") and ("
                + relVariable.getDstNode().getVariableName() + ") have already been matched in " +
                "previous query part.");
        }
    }

    private static void validateNoParallelRels(Schema currentMatchGraphSchema,
        RelVariable relVariable) {
        if (currentMatchGraphSchema.containsNodeVarName(relVariable.getSrcNode()
            .getVariableName()) &&
            MatchGraphSchemaUtils.getRelVariables(currentMatchGraphSchema, relVariable.getSrcNode(),
                relVariable.getDstNode()).size() > 0) {
            throw new MalformedQueryException("Parallel edge matching detected in the query. " +
                " Query vertices: (" + relVariable.getSrcNode().getVariableName() + ") and " +
                "(" + relVariable.getDstNode().getVariableName() + ") are being matched twice " +
                "in the same query part.");
        }
    }

    private static void validateRelVarIsNotAlreadyMatched(QueryPart currentQPart,
        RelVariable relVariable) {
        validateRelVariableIsNotInSchema(currentQPart.getInputSchema(), relVariable);
        validateRelVariableIsNotInSchema(currentQPart.getMatchGraphSchema(), relVariable);
    }

    private static void validateRelVariableIsNotInSchema(Schema inputSchema, RelVariable relVariable) {
        if (inputSchema.containsRelVarName(relVariable.getVariableName())) {
            throw new MalformedQueryException("RelVariable " + relVariable + " is being " +
                "specified to refer to two edges. Currently each query edge variable can be used " +
                "to refer to a single edge.");
        }
        if (inputSchema.containsVarName(relVariable.getVariableName())) {
            throw new MalformedQueryException("SimpleVariable " + relVariable + " is being reused " +
                "to refer to a query edge. Variables for query edges cannot be reused to " +
                "refer to any data in the current or any previous query part.");
        }
    }

    private static void validateNodeVariablesHaveDataTypeNodeIfAlreadyMatched(
        QueryPart currentQPart, RelVariable relVariable) {
        validateDataTypeIsNodeIfInSchema(currentQPart.getInputSchema(),
            relVariable.getSrcNode().getVariableName());
        validateDataTypeIsNodeIfInSchema(currentQPart.getMatchGraphSchema(),
            relVariable.getSrcNode().getVariableName());
        validateDataTypeIsNodeIfInSchema(currentQPart.getInputSchema(),
            relVariable.getDstNode().getVariableName());
        validateDataTypeIsNodeIfInSchema(currentQPart.getMatchGraphSchema(),
            relVariable.getDstNode().getVariableName());

    }

    private static void validateDataTypeIsNodeIfInSchema(Schema schema, String nodeVarName) {
        if (schema.containsVarName(nodeVarName) &&
            (DataType.NODE != schema.getExpression(nodeVarName).getDataType())) {
            throw new MalformedQueryException("NodeVarName " + nodeVarName + " has a different " +
                "dataType in previous reference: " + schema.getExpression(nodeVarName).getDataType());
        }
    }
}
