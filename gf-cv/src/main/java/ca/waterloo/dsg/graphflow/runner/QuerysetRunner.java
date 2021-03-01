package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.parser.QueryParser;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.plan.RegularQueryPlan;
import ca.waterloo.dsg.graphflow.planner.enumerators.RegularQueryPlanEnumerator;
import ca.waterloo.dsg.graphflow.runner.QuerysetRunner.QuerysetQueries.Query;
import ca.waterloo.dsg.graphflow.runner.utils.ArgsFactory;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.commons.cli.Options;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Benchmarks the system on a dataset. Reports time, memory usage and other relevant metrics for
 * loading the dataset, creating indices and executing queries that span over entire operator set
 * of Graphflow. Generates the report in the `benchmark` folder.
 * */
public class QuerysetRunner extends AbstractPlanRunner {

    protected static class QuerysetQueries {

        protected static class Query {
            String query;
            boolean execute;
            int planIdx;
        }

        Query[] queries;
    }

    static int runs;
    static int warmUpRuns;

    public static void main(String[] args) throws Exception {
        if (isAskingHelp(args, getCommandLineOptions())) {
            return;
        }
        var cmdLine = parseCommandLine(args, getCommandLineOptions());
        if (null == cmdLine) {
            throw new IllegalArgumentException("Incomplete arguments list.");
        }
        var inputDir = sanitizeDirStr(cmdLine.getOptionValue(ArgsFactory.INPUT_DIR));
        runs = cmdLine.hasOption(ArgsFactory.NUM_RUNS) ?
            Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.NUM_RUNS)) :
            2 /* default number of runs*/;
        warmUpRuns = cmdLine.hasOption(ArgsFactory.WARM_UP_RUNS) ?
            Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.WARM_UP_RUNS)) :
            1 /* default number of warmUp runs */;
        var loadDatasetStats = new ArrayList<Double>();
        loadDataset(inputDir, loadDatasetStats);
        var queryset = sanitizeDirStr(cmdLine.getOptionValue(ArgsFactory.BENCHMARK_QUERYSET));
        var queryExecutionStats = new ArrayList<QueryExecutionStat>();
        executeQueries(queryset.substring(0, queryset.length() - 1), queryExecutionStats);
        printStats(loadDatasetStats, queryExecutionStats);
    }

    static QuerysetQueries readQueriesFile(String queryset) throws IOException {
        var querySetFilename = queryset + ".queries";
        if (!(new File(querySetFilename)).exists()) {
            logger.info(querySetFilename);
            throw new FileNotFoundException("queries file is not found.");
        }
        return new Gson().fromJson(new JsonReader(new StringReader(
                Files.readString(Paths.get(querySetFilename), StandardCharsets.US_ASCII))),
            QuerysetQueries.class);
    }

    static List<RegularQueryPlan> getPlans(Query query) {
        var parsedQuery = (RegularQuery) QueryParser.parseQuery(query.query,
            graph.getGraphCatalog());
        return new RegularQueryPlanEnumerator(parsedQuery, graph)
            .enumeratePlansForQuery();
    }

    private static void executeQueries(String querysetName, List<QueryExecutionStat> stats)
        throws Exception {
        var queryset = readQueriesFile(querysetName);
        for (var query: queryset.queries) {
            if (query.execute) {
                var plans = getPlans(query);
                if (query.planIdx == -1) {
                    var queryExecutionStats = new ArrayList<QueryExecutionStat>(plans.size());
                    executeAllPlans(queryExecutionStats, plans, warmUpRuns, runs);
                    stats.addAll(queryExecutionStats);
                } else {
                    var plan = plans.get(query.planIdx);
                    var queryExecutionStat = new QueryExecutionStat();
                    queryExecutionStat.query = query;
                    executeASingleQueryPlan(queryExecutionStat, plan, warmUpRuns, runs);
                    stats.add(queryExecutionStat);
                }
            }
        }
    }

    private static void printStats(List<Double> loadDatasetStats, List<QueryExecutionStat> stats)
        throws IOException {
        var catalog = graph.getGraphCatalog();
        var writer = getOutputFileWriter();
        var joiner = new StringJoiner("\n");
        joiner.add("\nDATASET CHARACTERISTICS\n");
        joiner.add(String.format("#vertices: %d, #edges: %d, #type: %d, #labels: %d, " +
                "#NodesProperties: %d, #RelProperties: %d", graph.getNumNodes(),
            graph.getNumRels(), catalog.getNumTypes(), catalog.getNumLabels(),
            catalog.getNextNodePropertyKey(), catalog.getNextRelPropertyKey()));
//        graph.getGraphCatalog().print();
        joiner.add("\nDATASET LOAD STATS\n");
        joiner.add("----------------------------------------");
        joiner.add(String.format("Nodes Store deserialization : %.3fms", loadDatasetStats.get(0)));
        joiner.add(String.format("Nodes Store Size            : %.3f MB", loadDatasetStats.get(1)));
        joiner.add("----------------------------------------");
        joiner.add(String.format("Rel Store deserialization   : %.3fms", loadDatasetStats.get(2)));
        joiner.add(String.format("Rel Store Size              : %.3f MB", loadDatasetStats.get(3)));
        joiner.add("----------------------------------------");
        joiner.add(String.format("FWD indexes deserialization  : %.3fms", loadDatasetStats.get(4)));
        joiner.add(String.format("FWD indexes Size             : %.3f MB", loadDatasetStats.get(5)));
        joiner.add("----------------------------------------");
        joiner.add(String.format("BWD indexes deserialization  : %.3fms", loadDatasetStats.get(6)));
        joiner.add(String.format("BWD indexes Size             : %.3f MB", loadDatasetStats.get(7)));
        joiner.add("----------------------------------------");
        joiner.add(String.format("Rest of Graph deserialization: %.3fms", loadDatasetStats.get(8)));
        joiner.add(String.format("Rest of Graph Size           : %.3f MB", loadDatasetStats.get(9)));
        joiner.add("----------------------------------------");

        if (null != stats && stats.size() > 0) {
            joiner.add("\nREGULAR QUERY STAT");
            var i = 0;
            for (var stat : stats) {
                joiner.add(i++ + ": --------------------------------------");
                joiner.add(stat.toString());
            }
        }
        System.out.println(joiner.toString());
        writer.print(joiner.toString());
        writer.flush();
        writer.close();
    }

    private static PrintWriter getOutputFileWriter() throws IOException {
        var time = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH:mm:ss").format(LocalDateTime.now());
        var path = "../benchmark/" + String.format("report_%s", time);
        var file = new File(path);
        System.out.println(file.getAbsolutePath());
        return new PrintWriter(new FileWriter(file.getPath()));
    }

    private static Options getCommandLineOptions() {
        var options = new Options();
        options.addOption(ArgsFactory.getInputGraphDirectoryOption());    // INPUT_DIR          -i
        options.addOption(ArgsFactory.getBenchmarkQuerySetOption());      // BENCHMARK_DIR      -b
        options.addOption(ArgsFactory.getNumberRunsOptions());            // NUM_RUNS           -r
        options.addOption(ArgsFactory.getWarmupRunsOption());             // DISABLE_WARMUP     -w
        return options;
    }
}