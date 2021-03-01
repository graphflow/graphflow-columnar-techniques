package ca.waterloo.dsg.graphflow.planner.enumerators;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.expressionevaluator.Filter;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;

public abstract class AbstractScanOrExtendEnumerator extends AbstractEnumerator {
    SubqueryPlansCache subqueryPlansCache;

    public AbstractScanOrExtendEnumerator(SubqueryPlansCache subqueryPlansCache, Graph graph) {
        this(graph);
        this.subqueryPlansCache = subqueryPlansCache;
    }

    public AbstractScanOrExtendEnumerator(Graph graph) {
        super(graph);
    }

    /**
     * This function extends a previous partial query plan by adding another {@link Operator}
     * to it. A {@link Filter} operator might also be added at the end depending of if there
     * are any unresolved predicates available.
     *
     * @param estimatedICost The estimated iCost of the new {@link SingleQueryPlan}.
     * @param estimatedNumOutTuples The estimated output tuples of the new {@link SingleQueryPlan}.
     * @param nextOp The next physical {@link Operator}.
     * @param prevQueryPlan A previous partial {@link SingleQueryPlan} which this plan is extending.
     * @param remainingPredicate The remaining predicate that must go into the last {@link Filter}
     * operator.
     * @return A new {@link SingleQueryPlan} object.
     */
    SingleQueryPlan getNewQueryPlan(double estimatedICost, double estimatedNumOutTuples,
        Operator nextOp, SingleQueryPlan prevQueryPlan, Expression remainingPredicate) {
        var newQueryPlan = prevQueryPlan.getShallowCopy();
        newQueryPlan.setEstimatedICost(estimatedICost);
        newQueryPlan.setEstimatedNumOutTuples(estimatedNumOutTuples);
        newQueryPlan.append(nextOp);
        if (null != remainingPredicate) {
            appendFilter(newQueryPlan, remainingPredicate);
        }
        return newQueryPlan;
    }
}
