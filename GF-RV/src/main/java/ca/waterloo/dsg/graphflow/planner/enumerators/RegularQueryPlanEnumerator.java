package ca.waterloo.dsg.graphflow.planner.enumerators;

import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.plan.RegularQueryPlan;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.sink.AbstractSink;
import ca.waterloo.dsg.graphflow.plan.operator.sink.AbstractUnion;
import ca.waterloo.dsg.graphflow.plan.operator.sink.Sink;
import ca.waterloo.dsg.graphflow.plan.operator.sink.Union;
import ca.waterloo.dsg.graphflow.plan.operator.sink.UnionAll;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;


public class RegularQueryPlanEnumerator extends AbstractEnumerator {

    private Logger logger = LogManager.getLogger(RegularQueryPlanEnumerator.class);
    private RegularQuery query;

    public RegularQueryPlanEnumerator(RegularQuery query, Graph graph) {
        super(graph);
        this.query = query;
    }

    public List<RegularQueryPlan> enumeratePlansForQuery() {
        List<RegularQueryPlan> regularQueryPlans = new ArrayList<>();
        List<List<SingleQueryPlan>> singleQueryPlanLists = new ArrayList<>();
        Schema schema = query.singleQueries.get(0).queryParts.get(
            query.singleQueries.get(0).queryParts.size() - 1).getOutputSchema();
        query.singleQueries.forEach(singleQuery -> {
            singleQueryPlanLists.add(new SingleQueryPlanEnumerator(
                singleQuery, graph).enumeratePlansForQuery());
        });
        List<List<SingleQueryPlan>> singleQueryPlans = Lists.cartesianProduct(singleQueryPlanLists);
        for (int i = 0; i < singleQueryPlans.size(); ++i) {
            RegularQueryPlan regularQueryPlan = new RegularQueryPlan();
            regularQueryPlan.setSingleQueryPlans(singleQueryPlans.get(i));
            if (null == query.getUnionType()) {
                regularQueryPlan.appendSink(new Sink(schema));
                ((AbstractSink) regularQueryPlan.getLastOperator()).setDummyCopy(true);
            } else {
                AbstractUnion abstractUnion;
                if (AbstractUnion.UnionType.UNION_ALL == query.getUnionType()) {
                    abstractUnion = new UnionAll(schema);
                } else {
                    abstractUnion = new Union(schema);
                }
                regularQueryPlan.appendUnion(abstractUnion);
            }
            regularQueryPlans.add(regularQueryPlan);
        }
        return regularQueryPlans;
    }
}
