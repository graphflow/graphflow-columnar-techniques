package ca.waterloo.dsg.graphflow.planner.enumerators;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Stores plans for sub-queries during query optimization.
 */
public class SubqueryPlansCache {

    private Map<Integer /* level: #(qVertices) covered */,
        Map<String /* key for query vertices covered */, List<SingleQueryPlan>>> subgraphPlans;

    public SubqueryPlansCache() {
        this.subgraphPlans = new HashMap<>();
        subgraphPlans.putIfAbsent(0, new HashMap<>());
        subgraphPlans.get(0).put("", new ArrayList<>(Arrays.asList(new SingleQueryPlan())));
    }

    public void initializeForNextQPart(Set<NodeVariable> nodeVariables) {
        var lastQPartPlans = getPlansForLargestSubquery();
        subgraphPlans.clear();
        addPlans(nodeVariables, lastQPartPlans);
    }

    public List<SingleQueryPlan> getPlansForLargestSubquery() {
        return subgraphPlans.entrySet().stream().max(Map.Entry.comparingByKey()).get()
            .getValue().values().iterator().next();
    }

    public void addPlans(Collection<NodeVariable> nodeVariables, List<SingleQueryPlan> plans) {
        if (plans.isEmpty()) {
            return;
        }
        var numQVertices = nodeVariables.size();
        var key = getKey(nodeVariables);
        insertMapAndPlanListIfAbsent(numQVertices, key);
        subgraphPlans.get(numQVertices).get(key).addAll(plans);
    }

    Set<Entry<String, List<SingleQueryPlan>>> getEntriesForNumQVertices(int numQVertices) {
        return subgraphPlans.get(numQVertices).entrySet();
    }

    private void insertMapAndPlanListIfAbsent(int numQVertices, String key) {
        if (!subgraphPlans.containsKey(numQVertices)) {
            subgraphPlans.putIfAbsent(numQVertices, new HashMap<>());
        }
        if (!subgraphPlans.get(numQVertices).containsKey(key)) {
            subgraphPlans.get(numQVertices).put(key, new ArrayList<>());
        }
    }

    @SuppressWarnings("unchecked")
    public List<SingleQueryPlan> getPlansForSubquery(Collection<NodeVariable> nodeVariables) {
        var numQVertices = nodeVariables.size();
        var key = getKey(nodeVariables);
        if (!subgraphPlans.containsKey(numQVertices) ||
            !subgraphPlans.get(numQVertices).containsKey(key)) {
            return Collections.EMPTY_LIST;
        }
        return subgraphPlans.get(numQVertices).get(key);
    }

    static String getKey(Collection<NodeVariable> nodeVariables) {
        return nodeVariables.stream().map(Expression::getVariableName).sorted()
            .collect(Collectors.joining(","));
    }
}
