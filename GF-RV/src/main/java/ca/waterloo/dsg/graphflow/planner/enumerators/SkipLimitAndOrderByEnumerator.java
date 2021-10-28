package ca.waterloo.dsg.graphflow.planner.enumerators;

import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.ReturnBody;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.limit.Limit;
import ca.waterloo.dsg.graphflow.plan.operator.orderby.OrderBy;
import ca.waterloo.dsg.graphflow.plan.operator.skip.Skip;

import java.util.List;

public class SkipLimitAndOrderByEnumerator {

    public void appendSkipLimitOrderByAndRemoveFPlansIfNecessary(
        List<SingleQueryPlan> prevPlans, ReturnBody returnBody) {
        if (!returnBody.getOrderByConstraints().isEmpty()) {
            prevPlans.forEach(plan -> {
                plan.append(new OrderBy(returnBody.getOrderByConstraints(),
                    plan.getLastOperator().getOutSchema()));
            });
        }
        if (-1 != returnBody.getNumTuplesToSkip()) {
            prevPlans.forEach(plan -> {
                plan.append(new Skip(returnBody.getNumTuplesToSkip(), plan.getLastOperator().getOutSchema()));
            });
        }
        if (-1 != returnBody.getNumTuplesToLimit()) {
            prevPlans.forEach(plan -> {
                plan.append(new Limit(returnBody.getNumTuplesToLimit(), plan.getLastOperator().getOutSchema()));
            });
        }
    }
}
