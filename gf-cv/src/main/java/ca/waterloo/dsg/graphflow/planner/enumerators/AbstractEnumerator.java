package ca.waterloo.dsg.graphflow.planner.enumerators;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.query.expressions.BooleanConnectorExpression.ANDExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.ComparisonExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.plan.RegularQueryPlan;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator.Filter;
import ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator.FlatFilter;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.RelPropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.PropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.NodePropertyReader;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractEnumerator {

    Graph graph;

    public AbstractEnumerator(Graph graph) {
        this.graph = graph;
    }

    void appendPropertyReaderToPlan(SingleQueryPlan plan, PropertyVariable propertyVariable,
        Graph graph) {
        PropertyReader propertyReader;
        var nodeOrRelVariable = propertyVariable.getNodeOrRelVariable();
        switch (nodeOrRelVariable.getDataType()) {
            case NODE:
                propertyReader = NodePropertyReader.make(propertyVariable, graph.getNodePropertyStore(),
                    plan.getLastOperator().getOutSchema());
                break;
            case RELATIONSHIP:
                propertyReader = RelPropertyReader.make(propertyVariable, graph.getRelPropertyStore(),
                    plan.getLastOperator().getOutSchema(), graph.getGraphCatalog());
                break;
            default:
                throw new UnsupportedOperationException("Cannot read the property of a" +
                    " variable that is not of type vertex  or edge." +
                    " PropertyVariable: " + propertyVariable.getVariableName() +
                    " NodeOrRelVariable: " + nodeOrRelVariable.getVariableName() +
                    " data type: " + nodeOrRelVariable.getDataType().name() +
                    " propertyToRead: " + propertyVariable.getPropertyName());
        }
        plan.append(propertyReader);
    }

    public void appendFilterOrRemovePlanIfFilterCannotBeAppended(List<SingleQueryPlan> plans,
        Expression predicate) {
        plans.forEach(plan -> appendFilter(plan, predicate));
    }

    /**
     * Appends a {@link Filter} operator to the given plan to evaluate the given predicate.
     *
     * Warning: This method assumes that the given predicate has dependency on 0 or only 1 fGroup.
     *
     * @param plan The query plan which may require a Filter operator.
     * @param predicateToEvaluate A boolean predicate that should be executed by the Filter
     * operator.
     */
    public void appendFilter(SingleQueryPlan plan, Expression predicateToEvaluate) {
        // TODO: predicatesToEvaluate should be sorted based on some selectivity estimate.
        var dependentPropertyVariables = predicateToEvaluate.getDependentPropertyVariables();
        var lastOpOutSchema = plan.getLastOperator().getOutSchema();
        for (var dependentPropertyVariable : dependentPropertyVariables) {
            if (!lastOpOutSchema.containsVarName(dependentPropertyVariable.getVariableName())) {
                appendPropertyReaderToPlan(plan, dependentPropertyVariable, graph);
            }
        }

        var inSchema = plan.getLastOperator().getOutSchema();
        plan.append(new FlatFilter(predicateToEvaluate, inSchema));

        var numOutputTuples = plan.getEstimatedNumOutTuples() * 1.0 /*predicate selectivity*/;
        plan.setEstimatedNumOutTuples(numOutputTuples);
    }
}
