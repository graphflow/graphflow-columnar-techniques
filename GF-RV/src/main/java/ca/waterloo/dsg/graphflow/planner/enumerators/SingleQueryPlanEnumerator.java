package ca.waterloo.dsg.graphflow.planner.enumerators;

import ca.waterloo.dsg.graphflow.parser.query.expressions.ExpressionUtils;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.InputSchemaMatchWhere;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.SingleQuery;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.util.collection.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class SingleQueryPlanEnumerator extends AbstractScanOrExtendEnumerator {
    private static final Logger logger = LogManager.getLogger(SingleQueryPlanEnumerator.class);

    private SingleQuery query;
    private ScanNodeEnumerator scanNodeEnumerator;
    private EIEnumerator eiEnumerator;
    private ProjectionOrGroupByAndAggregateEnumerator projectionOrGroupByAndAggregateEnumerator;
    private SkipLimitAndOrderByEnumerator skipLimitAndOrderByEnumerator;

    public SingleQueryPlanEnumerator(SingleQuery query, Graph graph) {
        super(graph);
        this.subqueryPlansCache = new SubqueryPlansCache();
        this.query = query;
        this.scanNodeEnumerator = new ScanNodeEnumerator(subqueryPlansCache, graph);
        this.eiEnumerator = new EIEnumerator(subqueryPlansCache, graph);
        this.projectionOrGroupByAndAggregateEnumerator =
            new ProjectionOrGroupByAndAggregateEnumerator(graph);
        this.skipLimitAndOrderByEnumerator = new SkipLimitAndOrderByEnumerator();
    }

    public List<SingleQueryPlan> enumeratePlansForQuery() {
        for (int i = 0; i < query.queryParts.size(); ++i) {
            var currentQpart = query.queryParts.get(i);
            Set<NodeVariable> alreadyMatchedNodeVariables;
            if (i == 0) {
                alreadyMatchedNodeVariables = new HashSet<>();
            } else {
                alreadyMatchedNodeVariables = SetUtils.intersect(
                    query.queryParts.get(i-1).getOutputSchema().getNodeVariables(),
                    currentQpart.getMatchGraphSchema().getNodeVariables());
            }
            subqueryPlansCache.initializeForNextQPart(alreadyMatchedNodeVariables);
            enumeratePlansForMatchAndWhere(currentQpart.getInputSchemaMatchWhere(), alreadyMatchedNodeVariables);
            var prevPlans = subqueryPlansCache.getPlansForLargestSubquery();
            projectionOrGroupByAndAggregateEnumerator.appendProjectionOrGroupByAndAggregateIfNecessary(
                prevPlans, currentQpart.getReturnOrWith());
            prevPlans = subqueryPlansCache.getPlansForLargestSubquery();
            skipLimitAndOrderByEnumerator.appendSkipLimitOrderByAndRemoveFPlansIfNecessary(
                prevPlans, currentQpart.getReturnBody());
        }
        var finalPlans = subqueryPlansCache.getPlansForLargestSubquery();
        return finalPlans;
    }

    private void enumeratePlansForMatchAndWhere(InputSchemaMatchWhere currentQPartMWGraph,
        Set<NodeVariable> NodeVariablesMatchedForCurrentQpart) {
        // As the first thing, we check if there is any predicate in the query part
        // we can apply directly, without doing any scans or extends, i.e., we check whether
        // there is any subExpression in the WHERE clause that only depends on the inputSchema.
        if (currentQPartMWGraph.hasWhereExpression()) {
            var predicateThatOnlyDependsOnInput =
                ExpressionUtils.projectOntoVars(currentQPartMWGraph.getWhereExpression(),
                    currentQPartMWGraph.getInputSchema());
            if (null != predicateThatOnlyDependsOnInput) {
                var itr = subqueryPlansCache.getEntriesForNumQVertices(
                    NodeVariablesMatchedForCurrentQpart.size()).iterator();
                while (itr.hasNext()) {
                    var keyPrevPlans = itr.next();
                    appendFilterOrRemovePlanIfFilterCannotBeAppended(keyPrevPlans.getValue(),
                        predicateThatOnlyDependsOnInput);
                    if (keyPrevPlans.getValue().isEmpty()) {
                        throw new IllegalStateException("This should never happen! We are " +
                            "removing all of the plans for sub-query Q_k: " + keyPrevPlans.getKey());
                    }
                }
            }
        }
        for (int nextNumVertices = NodeVariablesMatchedForCurrentQpart.size()+1;
            nextNumVertices <= currentQPartMWGraph.getMatchGraphSchema().getNodeVariables()
                .size(); ++nextNumVertices) {
            if (1 == nextNumVertices) {
                scanNodeEnumerator.extendPlansWithNextNodeScanOperators(currentQPartMWGraph);
            } else {
                // TODO: We scan edges if and only if this sub-query has no dependency on the
                // previous sub-query, so its results are effectively a Cartesian Product of the
                // previous sub-query with its own results. However currently Cartesian products
                // are not implemented. We generate the right plans, but Scan operators do not
                // implement {@link Operator.processNewTuple} method, which is needed for a correct
                // implementation.
                var prevNumQVertices = nextNumVertices - 1;
                for (Entry<String, List<SingleQueryPlan>> keyPrevPlans :
                    subqueryPlansCache.getEntriesForNumQVertices(prevNumQVertices)) {
                    eiEnumerator.extendPlansWithEIOperators(currentQPartMWGraph,
                        keyPrevPlans.getValue());
                }
            }
        }
    }
}
