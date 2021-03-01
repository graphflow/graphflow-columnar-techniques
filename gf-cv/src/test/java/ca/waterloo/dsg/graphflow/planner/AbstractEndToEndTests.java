package ca.waterloo.dsg.graphflow.planner;

import ca.waterloo.dsg.graphflow.parser.QueryParser;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.plan.RegularQueryPlan;
import ca.waterloo.dsg.graphflow.planner.enumerators.RegularQueryPlanEnumerator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Table;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.util.DataLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.util.HashSet;
import java.util.List;

public abstract class AbstractEndToEndTests {
    static Graph graphTinySnb;

    @BeforeAll
    public static void setUp() {
        graphTinySnb = DataLoader.getDataset("tiny-snb").graph;
    }

    void testAllPlans(String queryStr, int expectedCount) {
        getAllPlans(queryStr).forEach(plan -> {
            System.out.println(plan.toString());
            plan.init(graphTinySnb);
            plan.execute();
            System.out.println(plan);
            Assertions.assertEquals(expectedCount, plan.getNumOutputTuples());
        });
    }

    List<RegularQueryPlan> getAllPlans(String queryStr) {
        var query = (RegularQuery) QueryParser.parseQuery(queryStr, graphTinySnb.getGraphCatalog());
        var enumerator = new RegularQueryPlanEnumerator(query, graphTinySnb);
        return enumerator.enumeratePlansForQuery();
    }

    List<RegularQueryPlan> getAllPlansWithTupleStoringSink(String queryStr) {
        var plans = getAllPlans(queryStr);
        plans.forEach(plan -> plan.setStoreTuples(true));
        return plans;
    }

    protected void testUniqueValues(List<Value> vals) {
        var hashedValues = new HashSet<Value>();
        vals.forEach(value -> {
            Assertions.assertFalse(hashedValues.contains(value));
            hashedValues.add(value);
        });
    }

    protected void assertTest(String queryStr, Tuple sampleTupleForTable,
        List<Value[]> tupleValues) {
        var plans = getAllUnfactorizedPlans(queryStr);
        var expectedTable = Table.constructTableForTest(sampleTupleForTable, tupleValues);
        plans.forEach(plan -> {
            plan.setStoreTuples(true);
            plan.init(graphTinySnb);
            plan.execute();
            Table table = plan.getOutputTable();
            Assertions.assertTrue(expectedTable.isSame(table));
        });
    }

    protected List<RegularQueryPlan> getAllUnfactorizedPlans(String queryStr) {
        var query = (RegularQuery) QueryParser.parseQuery(queryStr, graphTinySnb.getGraphCatalog());
        var enumerator = new RegularQueryPlanEnumerator(query, graphTinySnb);
        var plans = enumerator.enumeratePlansForQuery();
        return plans;
    }
}
