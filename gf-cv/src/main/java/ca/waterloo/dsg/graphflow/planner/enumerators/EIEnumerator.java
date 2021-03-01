package ca.waterloo.dsg.graphflow.planner.enumerators;

import ca.waterloo.dsg.graphflow.parser.query.singlequery.MatchGraphSchemaUtils;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.InputSchemaMatchWhere;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator.Filter;
import ca.waterloo.dsg.graphflow.plan.operator.extend.Extend;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.util.collection.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;

public class EIEnumerator extends AbstractExtendEnumerator {

    private static final Logger logger = LogManager.getLogger(EIEnumerator.class);

    public EIEnumerator(SubqueryPlansCache subqueryPlansCache, Graph graph) {
        super(subqueryPlansCache, graph);
    }

    /**
     * Considers all possible single vertex extensions from the available query plans
     * for a given set of n vertices to n + 1 vertices using {@link Extend} operator. Any
     * remaining predicates after the E/I are encapsulated in a following {@link Filter} operator.
     *
     * @param currentMWGraph Current sub-query for which plans are being enumerated.
     * @param prevPlans The previously generated plans for any set of matched vertices. Each
     * previous plan denotes a specific vertex ordering.
     */
    void extendPlansWithEIOperators(InputSchemaMatchWhere currentMWGraph,
        List<SingleQueryPlan> prevPlans) {
        var alreadyMatchedNodesInCurrentQueryPart = SetUtils.intersect(
            prevPlans.get(0).getLastOperator().getOutSchema().getNodeVariables(),
            currentMWGraph.getMatchGraphSchema().getNodeVariables());
        var toNodeVariables = MatchGraphSchemaUtils.getNeighborNodeVariables(
            currentMWGraph.getMatchGraphSchema(), alreadyMatchedNodesInCurrentQueryPart);
        for (var toNodeVariable : toNodeVariables) {
            for (var prevQueryPlan : prevPlans) {
                var lastOperator = prevQueryPlan.getLastOperator();
                var inMWGraph = currentMWGraph.getProjection(
                    lastOperator.getOutSchema().getNodeVariables());
                var outMWNodeVariable = SetUtils.merge(inMWGraph.getMatchGraphSchema().
                    getNodeVariables(), toNodeVariable);
                var outMWGraph = currentMWGraph.getProjection(new HashSet<>(outMWNodeVariable));
                var ALDs = generateALDs(inMWGraph.getMatchGraphSchema(),
                    outMWGraph.getMatchGraphSchema());
                var qVerticesInSubquery = SetUtils.merge(
                    alreadyMatchedNodesInCurrentQueryPart, toNodeVariable);
                var selectivity = 1.0;
                for (var ALD : ALDs) {
                    selectivity *= getALIndexSelectivity(ALD, ALD.getRelVariable());
                }
                if (ALDs.size() == 1) {
                    subqueryPlansCache.addPlans(qVerticesInSubquery, getFlatExtendPlans(
                        ALDs.get(0), outMWGraph, inMWGraph, prevQueryPlan, selectivity));
                } else if (ALDs.size() == 2) {
                    subqueryPlansCache.addPlans(qVerticesInSubquery, getFlatIntersectPlan(
                        ALDs, outMWGraph, inMWGraph, prevQueryPlan, selectivity));
                } else {
                    throw  new IllegalArgumentException("do not support more that 2 alds.");
                }
            }
        }
    }
}
