package ca.waterloo.dsg.graphflow.planner.enumerators;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.scan.Scan;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;

public abstract class AbstractScanEnumerator extends AbstractScanOrExtendEnumerator {

    public AbstractScanEnumerator(SubqueryPlansCache subqueryPlansCache, Graph graph) {
        super(subqueryPlansCache, graph);
    }

    SingleQueryPlan appendScanBPIToPreviousPlan(Scan scan, Expression remainingExpression,
        double baseNumOutTuples, SingleQueryPlan prevPlan) {
        var estimatedScannedTuples = baseNumOutTuples * 1.0; /* selectedComparisonExpr selectivity*/
        return appendScanToPreviousQueryPlan(scan, remainingExpression,
            estimatedScannedTuples, prevPlan);
    }

    SingleQueryPlan appendScanToPreviousQueryPlan(Scan scan, Expression remainingPredicate,
        double estimatedScannedTuples, SingleQueryPlan prevPlan) {
        var estimatedNumOutTuples = (prevPlan.getEstimatedNumOutTuples() > 0 ?
            prevPlan.getEstimatedNumOutTuples() : 1) * estimatedScannedTuples;
        // TODO: Ask Amine if this is the right thing to do. This is confusing in the case of a
        // Cartesian product, expressions say a ScanVertex can be added to a new plan and its output
        // tuples need to be multiplied but it's less clear what should be done to its estimated
        // i-cost.
        var estimatedICost = prevPlan.getEstimatedICost() + estimatedNumOutTuples;
        return getNewQueryPlan(estimatedICost, estimatedNumOutTuples, scan, prevPlan,
            remainingPredicate);
    }
}
