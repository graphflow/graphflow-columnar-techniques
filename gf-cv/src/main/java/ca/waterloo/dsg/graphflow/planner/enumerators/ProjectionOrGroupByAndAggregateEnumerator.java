package ca.waterloo.dsg.graphflow.planner.enumerators;

import ca.waterloo.dsg.graphflow.parser.query.expressions.AliasExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.FunctionInvocation;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.SimpleVariable;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.AggregationFunction;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.ReturnBody.GroupByAndAggregateExpressions;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.ReturnBody.ProjectionExpressions;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.WithWhere;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator.ExpressionEvaluatorWriter;
import ca.waterloo.dsg.graphflow.plan.operator.groupbyaggregate.GroupByAggregate;
import ca.waterloo.dsg.graphflow.plan.operator.projection.PlaceHolderProjection;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class ProjectionOrGroupByAndAggregateEnumerator extends AbstractEnumerator {

    private static final Logger logger = LogManager.getLogger(ProjectionOrGroupByAndAggregateEnumerator.class);

    public ProjectionOrGroupByAndAggregateEnumerator(Graph graph) {
        super(graph);
    }

    void appendProjectionOrGroupByAndAggregateIfNecessary(List<SingleQueryPlan> prevPlans,
        ReturnOrWith returnOrWith) {
        switch(returnOrWith.getReturnBody().getReturnBodyType()) {
            case RETURN_OR_WITH_STAR: case PROJECTION:
                // Put ProjectionPlaceHolder and optionally PropertyReadingOperator
                appendProjectionAndOptionallyPropertyReadingOperator(prevPlans,
                    returnOrWith.getReturnBody().getProjectionExpressions());
                break;
            case GROUP_BY_AND_AGGREGATE:
                appendGroupByAndAggregateAndOptionallyPropertyReadingOperator(prevPlans,
                    returnOrWith.getReturnBody().getGroupByAndAggregateExpressions());
                if (returnOrWith instanceof WithWhere) {
                    var with = ((WithWhere) returnOrWith);
                    if (with.hasWhereExpression()) {
                        appendFilterOrRemovePlanIfFilterCannotBeAppended(prevPlans,
                            ((WithWhere) returnOrWith).getWhereExpression());
                        if (prevPlans.isEmpty()) {
                            logger.error("PrevPlans is empty after calling" +
                                "appendFilterOrRemovePlanIfFilterCannotBeAppended in "
                                + this.getClass().getSimpleName() + ". This should never happen.");

                        }
                    }
                }
                break;
        }
    }

    private void appendGroupByAndAggregateAndOptionallyPropertyReadingOperator(
        List<SingleQueryPlan> prevPlans, GroupByAndAggregateExpressions groupByAndAggregateExpressions) {
        prevPlans.forEach(plan -> {
            // TODO: There is a known bug here that we cannot handle queries in which the
            //  the part of the expression that is not inside the aggregation expression
            //  depends on variables. It can only depend on literals. But we do not currently
            //  check of that in the code. We will error or return wrong outputs for such queries:
            //  E.g: MATCH (a)->(b) RETURN a, b.age*max(b.age) will (most likely) error
            var aggregationExpressions = groupByAndAggregateExpressions.getAggregationExpressions();
            var variableToAggregates = new ArrayList<String>();
            var aggregatedVariables = new ArrayList<String>();
            var functionInvocations = new ArrayList<FunctionInvocation>();
            aggregationExpressions.forEach(aggregationExpression -> {
                addMissingPropertyReadersForExpression(plan, aggregationExpression);
                var functionInvocation =
                    aggregationExpression.getDependentFunctionInvocations().iterator().next();
                functionInvocations.add(functionInvocation);
                var aggregatedVariable = getAggregatedVariableName(aggregationExpression);
                String variableToAggregate = GroupByAggregate.COUNT_STAR_VAR_NAME;
                if (AggregationFunction.COUNT_STAR != functionInvocation.getFunction()) {
                    variableToAggregate = functionInvocation.getExpression().getVariableName();
                    appendExpressionEvaluatorIfNecessary(plan, functionInvocation.getExpression());
                }
                variableToAggregates.add(variableToAggregate);
                aggregatedVariables.add(aggregatedVariable);
            });
            GroupByAggregate groupByAndAggregate;
            List<String> groupByVariableNames = new ArrayList<>();
            if (groupByAndAggregateExpressions.hasGroupByVariables()) {
                var groupByVariables = groupByAndAggregateExpressions.getGroupByExpressions();
                groupByVariables.forEach(groupByVariable -> {
                    addMissingPropertyReadersAndExpressionEvaluator(plan, groupByVariable);
                    groupByVariableNames.add(groupByVariable.getVariableName());
                });
            }
            groupByAndAggregate = GroupByAggregate.make(groupByVariableNames, variableToAggregates,
                aggregatedVariables, functionInvocations, plan.getLastOperator().getOutSchema());
            plan.append(groupByAndAggregate);
            if (hasAnyOutsideExpression(aggregationExpressions, plan)) {
                plan.append(new PlaceHolderProjection(plan.getLastOperator().getOutSchema(),
                        groupByAndAggregateExpressions.getExpressions()));
            }
            // Warning: For now we are putting in a magic constant that group-by and aggregations
            // decrease the number of outputs by half. We can be smarter here and understand what
            // is being grouped by and try to make a better estimate.
            var numOutputTuples = plan.getEstimatedNumOutTuples() * 0.5;
            plan.setEstimatedNumOutTuples(numOutputTuples);
        });
    }

    private String getAggregatedVariableName(Expression aggregatingExpression) {
        var functionInvocation =
            aggregatingExpression.getDependentFunctionInvocations().iterator().next();
        if (aggregatingExpression instanceof AliasExpression) {
            var aliasExpr = ((AliasExpression) aggregatingExpression);
            if (aliasExpr.getExpression() == functionInvocation) {
                return aliasExpr.getVariableName();
            }
        }
        return functionInvocation.getVariableName();
    }

    private boolean hasOutsideExpression(Expression aggregatingExpression) {
        var functionInvocation =
            aggregatingExpression.getDependentFunctionInvocations().iterator().next();
        if (aggregatingExpression == functionInvocation) {
            return false;
        }
        if (aggregatingExpression instanceof AliasExpression) {
            var aliasExpr = ((AliasExpression) aggregatingExpression);
            if (aliasExpr.getExpression() == functionInvocation) {
                return false;
            }
        }
        return true;
    }

    private boolean hasAnyOutsideExpression(List<Expression> aggregationExpressions, SingleQueryPlan plan) {
        var flag = false;
        for (int i = 0; i < aggregationExpressions.size(); ++i) {
            if (hasOutsideExpression(aggregationExpressions.get(i))) {
                plan.append(ExpressionEvaluatorWriter.constructExpressionEvaluatorOperator(
                    aggregationExpressions.get(i), plan.getLastOperator().getOutSchema()));
                flag = true;
            }
        }
        return flag;
    }

    private void appendProjectionAndOptionallyPropertyReadingOperator(
        List<SingleQueryPlan> prevPlans, ProjectionExpressions projectionExpressions) {
        prevPlans.forEach(plan -> {
            for (Expression projectionExpression :
                projectionExpressions.getProjectionExpressions()) {
                addMissingPropertyReadersAndExpressionEvaluator(plan, projectionExpression);
            }
            plan.append(new PlaceHolderProjection(plan.getLastOperator().getOutSchema(),
                projectionExpressions.getProjectionExpressions()));
        });
    }

    private void addMissingPropertyReadersAndExpressionEvaluator(SingleQueryPlan plan,
        Expression expression) {
        addMissingPropertyReadersForExpression(plan, expression);
        appendExpressionEvaluatorIfNecessary(plan, expression);
    }

    private void appendExpressionEvaluatorIfNecessary(SingleQueryPlan plan, Expression expression) {
        if (!plan.getLastOperator().getOutSchema().containsVarName(expression.getVariableName()) &&
            !(expression instanceof SimpleVariable) && !(expression instanceof PropertyVariable)) {
            plan.append(ExpressionEvaluatorWriter.constructExpressionEvaluatorOperator(
                expression, plan.getLastOperator().getOutSchema()));
        }
    }

    private void addMissingPropertyReadersForExpression(SingleQueryPlan plan,
        Expression expression) {
        if (!plan.getLastOperator().getOutSchema().containsVarName(expression.getVariableName())) {
            var dependentPropVars = expression.getDependentPropertyVariables();
            for (var dependentPropVar : dependentPropVars) {
                if (!plan.getLastOperator().getOutSchema().containsVarName(
                    dependentPropVar.getVariableName())) {
                    appendPropertyReaderToPlan(plan, dependentPropVar, graph);
                }
            }
        }
    }
}
