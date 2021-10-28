package ca.waterloo.dsg.graphflow.planner.enumerators;

import ca.waterloo.dsg.graphflow.parser.query.expressions.ExpressionUtils;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.InputSchemaMatchWhere;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.MatchGraphSchemaUtils;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.extend.Extend;
import ca.waterloo.dsg.graphflow.plan.operator.extend.FlatExtendDefaultAdjList;
import ca.waterloo.dsg.graphflow.plan.operator.intersect.IntersectLL;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.collection.SetUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractExtendEnumerator extends AbstractScanOrExtendEnumerator {

    public AbstractExtendEnumerator(SubqueryPlansCache subqueryPlansCache, Graph graph) {
        super(subqueryPlansCache, graph);
    }

    /**
     * This function assumes that the caller has checked that the given ald is not trying to
     * extend from a query vertex that is already in a factorized group in prevQueryPlan. For
     * example if the query was MATCH (a)->(b)->(c) and prevQueryPlan was ScanVertex (a),
     * FactorizedExtend to (b), then we cannot further FactorizedExtend from (b) to (c). This
     * check must have been made (at the time of writing, in
     * {@link EIEnumerator#extendPlansWithEIOperators(InputSchemaMatchWhere, List)}.
     * Given this assumption, this function extends a previous partial {@link SingleQueryPlan}
     * prevQueryPlan with one of two possibilities.
     *
     * (1) If after an extension, we will need to apply a predicate and that predicate depends
     * on multiple factorized groups, then we only extend prevQueryPlan with
     * {@link FlatExtendDefaultAdjList}.
     * (2) Otherwise we extend prevQueryPlan with both factorized and flat versions
     * of {@link Extend}.
     */
    List<SingleQueryPlan> getFlatExtendPlans(AdjListDescriptor ald,
        InputSchemaMatchWhere outMWGraph, InputSchemaMatchWhere inMWGraph,
        SingleQueryPlan prevQueryPlan, double selectivityMultiplier) {
        var newAld = ald.copy();
//        var prevNumOutTuples = prevQueryPlan.getEstimatedNumOutTuples();
//        var iCost = prevNumOutTuples * (5.0 /*i-cost from ALDs*/ * selectivityMultiplier);
//        var estimatedICost = prevQueryPlan.getEstimatedICost() + iCost;
//        var estimatedSelectivity = 1.0 /*structural selectivity*/ * selectivityMultiplier;
//        var estimatedNumOutTuples = prevNumOutTuples * estimatedSelectivity;
        var remainingPredicate = outMWGraph.getWhereExpression();
        if (outMWGraph.hasConjunctiveWhereExpression()) {
            remainingPredicate = ExpressionUtils.projectOutVars(outMWGraph.getWhereExpression(),
                inMWGraph.getMatchGraphSchema());
        }
        var retVal = new ArrayList<SingleQueryPlan>();
        var inSchemaToEI = prevQueryPlan.getLastOperator().getOutSchema();
        retVal.add(getNewQueryPlan(0.0 /*i-cost*/, 0.0 /*#tuples*/, Extend.makeFlat(
            newAld, inSchemaToEI, graph.getGraphCatalog()), prevQueryPlan, remainingPredicate));
        return retVal;
    }

    /**
     * Assuming only 2 ALDs.
     * */
    List<SingleQueryPlan> getFlatIntersectPlan(List<AdjListDescriptor> alds,
        InputSchemaMatchWhere outMWGraph, InputSchemaMatchWhere inMWGraph,
        SingleQueryPlan prevQueryPlan, double selectivityMultiplier) {
        var newAld1 = alds.get(0).copy();
        var newAld2 = alds.get(1).copy();
//        var prevNumOutTuples = prevQueryPlan.getEstimatedNumOutTuples();
//        var iCost = prevNumOutTuples * (5.0 /*i-cost from ALDs*/ * selectivityMultiplier);
//        var estimatedICost = prevQueryPlan.getEstimatedICost() + iCost;
//        var estimatedSelectivity = 1.0 /*structural selectivity*/ * selectivityMultiplier;
//        var estimatedNumOutTuples = prevNumOutTuples * estimatedSelectivity;
        var remainingPredicate = outMWGraph.getWhereExpression();
        if (outMWGraph.hasConjunctiveWhereExpression()) {
            remainingPredicate = ExpressionUtils.projectOutVars(outMWGraph.getWhereExpression(),
                inMWGraph.getMatchGraphSchema());
        }
        var retVal = new ArrayList<SingleQueryPlan>();
        var inSchemaToEI = prevQueryPlan.getLastOperator().getOutSchema();
        retVal.add(getNewQueryPlan(0.0 /*i-cost*/, 0 /*#tuples*/, IntersectLL.makeFlat(
            newAld1, newAld2, inSchemaToEI, graph.getGraphCatalog()), prevQueryPlan,
            remainingPredicate));
        return retVal;
    }

    /**
     * Takes an input the current match graph and an output match graph and generates a list of
     * ALDs which extend from the input match graph to the output match graph. Also extends the
     * vertex and edge indexquery maps to include the new vertices and edges for the respective ALDs.
     *
     * TODO(Semih): This may or may not be used. We may want to use it to make sure that we
     *  enumerate the edges but ensuring that we close cycles whenever possible.
     * @param inMatchGraphSchema The schema of input match graph.
     * @param outMatchGraphSchema The schema of output match graph.
     * @return The list of required ALDs for the extension.
     */
    List<AdjListDescriptor> generateALDs(Schema inMatchGraphSchema,
        Schema outMatchGraphSchema) {
        var boundNodeVariables = inMatchGraphSchema.getNodeVariables();
        var toNodeVariables = SetUtils.subtract(outMatchGraphSchema.getNodeVariables(),
            boundNodeVariables);
        var ALDs = new ArrayList<AdjListDescriptor>();
        for (var toNodeVariable : toNodeVariables) {
            for (var boundNodeVariable : boundNodeVariables) {
                for (var relVariable : MatchGraphSchemaUtils.getRelVariables(outMatchGraphSchema,
                    boundNodeVariable, toNodeVariable)) {
                    if (relVariable.getSrcNode().equals(boundNodeVariable)) {
                        ALDs.add(new AdjListDescriptor(relVariable, boundNodeVariable,
                            toNodeVariable, Direction.FORWARD));
                    } else {
                        ALDs.add(new AdjListDescriptor(relVariable, boundNodeVariable,
                            toNodeVariable, Direction.BACKWARD));
                    }
                }
            }
        }
        return ALDs;
    }

    double getALIndexSelectivity(AdjListDescriptor ALD, RelVariable relVariable) {
        double selectivity = 1.0;
        if (Direction.FORWARD == ALD.getDirection()) {
            selectivity *= graph.getNumNodes(relVariable.getSrcNode().getType()) /
                (double) graph.getNumNodes();
        } else if (Direction.BACKWARD == ALD.getDirection()) {
            selectivity *= graph.getNumNodes(relVariable.getDstNode().getType()) /
                (double) graph.getNumNodes();
        }
        selectivity *= graph.getNumRels(relVariable.getLabel()) / (double) graph.getNumRels();
        return selectivity;
    }
}
