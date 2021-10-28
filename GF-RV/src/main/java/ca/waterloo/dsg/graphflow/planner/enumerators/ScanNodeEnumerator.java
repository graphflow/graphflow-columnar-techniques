package ca.waterloo.dsg.graphflow.planner.enumerators;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.InputSchemaMatchWhere;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanNode;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class ScanNodeEnumerator extends AbstractScanEnumerator {

    private static final Logger logger = LogManager.getLogger(ScanNodeEnumerator.class);

    public ScanNodeEnumerator(SubqueryPlansCache subqueryPlansCache, Graph graph) {
        super(subqueryPlansCache, graph);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    void extendPlansWithNextNodeScanOperators(InputSchemaMatchWhere currentMWGraph) {
        for (var nodeVariable : currentMWGraph.getMatchGraphSchema().getNodeVariables()) {
            var qNodesInSubquery = new HashSet<NodeVariable>() {{ add(nodeVariable); }};
            var outMWGraph = currentMWGraph.getProjection(qNodesInSubquery);
            var prevPlans = subqueryPlansCache.getPlansForSubquery(Collections.EMPTY_SET);
//            var plansWithIndexes = appendScanNodeIndexToPreviousPlans(outMWGraph, nodeVariable,
//                prevPlans);
//            if (0 == plansWithIndexes.size()) {
//                subqueryPlansCache.addPlans(qNodesInSubquery,
//                    appendScanNodeToPreviousPlans(outMWGraph, nodeVariable, prevPlans));
//            } else {
//                subqueryPlansCache.addPlans(qNodesInSubquery, plansWithIndexes);
//            }
            var prevPlansWithScan = appendScanNodeToPreviousPlans(outMWGraph, nodeVariable,
                prevPlans);
            subqueryPlansCache.addPlans(qNodesInSubquery, prevPlansWithScan);
        }
    }

    private List<SingleQueryPlan> appendScanNodeToPreviousPlans(InputSchemaMatchWhere outMWGraph,
        NodeVariable nodeVariable, List<SingleQueryPlan> prevPlans) {
        // Warning: We are assuming that the input outGraph contains a single query vertex.
        var scan = new ScanNode((NodeVariable) outMWGraph.getMatchGraphSchema()
            .getVariablesInLexOrder().iterator().next().getValue());
        var plans = new ArrayList<SingleQueryPlan>();
        prevPlans.forEach(prevPlan -> plans.add(appendScanToPreviousQueryPlan(scan,
            outMWGraph.getWhereExpression(), graph.getNumNodes(
                nodeVariable.getType()), prevPlan)));
        return plans;
    }
}
