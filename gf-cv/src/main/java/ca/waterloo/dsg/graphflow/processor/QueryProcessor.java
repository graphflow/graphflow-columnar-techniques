package ca.waterloo.dsg.graphflow.processor;

import ca.waterloo.dsg.graphflow.parser.QueryParser;
import ca.waterloo.dsg.graphflow.parser.query.AbstractQuery;
import ca.waterloo.dsg.graphflow.parser.query.indexquery.CreateBPTNodeIndexQuery;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.QueryPart;
import ca.waterloo.dsg.graphflow.plan.SingleQueryPlan;
import ca.waterloo.dsg.graphflow.planner.enumerators.RegularQueryPlanEnumerator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class to process incoming queries.
 */
public class QueryProcessor {

    private static final Logger logger = LogManager.getLogger(QueryProcessor.class);

    @Getter private double parserElapsedTime;
    @Getter private double queryExecutionElapsedTime;

    /**
     * Executes a string query by converting it into a {@link QueryPart}, generating a
     * {@link SingleQueryPlan}, and executes the plan.
     *
     * @param request The {@code String} input request.
     */
    public void process(String request, Graph graph) {
        var beginTime = System.nanoTime();
        logger.info("Starting to execute request: " + request);
        AbstractQuery query;
        query = QueryParser.parseQuery(request, graph.getGraphCatalog());
        parserElapsedTime = IOUtils.getTimeDiff(beginTime);
        switch (query.getQueryOperation()) {
            case REGULAR_QUERY:
                var matchPlan = new RegularQueryPlanEnumerator((RegularQuery) query,
                    graph).enumeratePlansForQuery().get(0);
                matchPlan.init(graph);
                matchPlan.execute();
                break;
            case CREATE_BPLUS_TREE_NODE_INDEX:
                throw  new IllegalArgumentException("no index.");
            default:
                logger.error("'" + query.getQueryOperation() + "' is not defined.");
        }
        queryExecutionElapsedTime = IOUtils.getTimeDiff(beginTime);

        logger.info("Parsing runtime is " + parserElapsedTime + "(ms)");
        logger.info("Parsing runtime is " + queryExecutionElapsedTime + "(ms)");
    }
}
