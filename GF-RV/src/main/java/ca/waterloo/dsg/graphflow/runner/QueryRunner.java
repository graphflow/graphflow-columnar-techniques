package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.parser.QueryParser;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.plan.RegularQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.extend.FlatExtendDefaultAdjList;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanNode;
import ca.waterloo.dsg.graphflow.planner.enumerators.RegularQueryPlanEnumerator;
import ca.waterloo.dsg.graphflow.runner.utils.ArgsFactory;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class QueryRunner extends AbstractPlanRunner {

    protected static final Logger logger = LogManager.getLogger(QueryRunner.class);

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

        var queryExecutionStat = new QueryExecutionStat();
        var plan = resolvePlan(cmdLine.getOptionValue(ArgsFactory.BENCHMARK_QUERY), queryExecutionStat);

        executeASingleQueryPlan(queryExecutionStat, plan, warmUpRuns, runs);

        printStats(loadDatasetStats, queryExecutionStat);
    }

    private static RegularQueryPlan resolvePlan(String queryFilename, QueryExecutionStat qes) throws IOException {

        /* read query */
        if (!(new File(queryFilename)).exists()) {
            logger.info(queryFilename);
            throw new FileNotFoundException("Query file is not found.");
        }
        qes.query = new Gson().fromJson(new JsonReader(new StringReader(
                Files.readString(Paths.get(queryFilename), StandardCharsets.US_ASCII))), Query.class);

        /* get query plan */
        var parsedQuery = (RegularQuery) QueryParser.parseQuery(qes.query.qstr, graph.getGraphCatalog());
        var allPlans =  new RegularQueryPlanEnumerator(parsedQuery, graph)
                .enumeratePlansForQuery();

        // choose the plan using the QVO
        var qvo = qes.query.qvo;
        for (var plan: allPlans) {
            var toMatch = qvo.length - 1;
            var opr = plan.getLastOperator();
            while (opr != null) {
                if (opr instanceof FlatExtendDefaultAdjList.FlatExtendDefaultAdjListSingleType ||
                        opr instanceof FlatExtendDefaultAdjList.FlatExtendDefaultAdjListMultiType ||
                        opr instanceof ScanNode) {
                    String toNodeVar;
                    if (opr instanceof FlatExtendDefaultAdjList.FlatExtendDefaultAdjListSingleType ||
                            opr instanceof FlatExtendDefaultAdjList.FlatExtendDefaultAdjListMultiType) {
                        toNodeVar = ((FlatExtendDefaultAdjList) opr).getAld().getToNodeVariable().getVariableName();
                    } else {
                        toNodeVar = ((ScanNode) opr).getNodeName();
                    }
                    if (toMatch < 0) {
                        throw new IllegalArgumentException("Possibly wrong QVO array!");
                    }
                    if (!toNodeVar.equals(qvo[toMatch])) {
                        break;
                    }
                    toMatch--;
                }
                opr = opr.getPrev();
            }
            if (toMatch == -1) {
                System.out.println("chosen plan: " + plan.outputAsString());
                return plan;
            }
        }
        throw new IllegalArgumentException("Cannot find a matching plan!");
    }

    private static void printStats(List<Double> loadDatasetStats, QueryExecutionStat stat)
        throws IOException {
        var catalog = graph.getGraphCatalog();
        var joiner = new StringJoiner("\n");
        joiner.add("\nDATASET CHARACTERISTICS\n");
        joiner.add(String.format("#vertices: %d, #edges: %d, #type: %d, #labels: %d, " +
                "#NodesProperties: %d, #RelProperties: %d", graph.getNumNodes(),
            graph.getNumRels(), catalog.getNumTypes(), catalog.getNumLabels(),
            catalog.getNextNodePropertyKey(), catalog.getNextRelPropertyKey()));
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

        joiner.add("\nQUERY STAT");
        joiner.add(stat.toString());
        System.out.println(joiner);
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
        options.addOption(ArgsFactory.getBenchmarkQueryOption());      // BENCHMARK_DIR      -b
        options.addOption(ArgsFactory.getNumberRunsOptions());            // NUM_RUNS           -r
        options.addOption(ArgsFactory.getWarmupRunsOption());             // DISABLE_WARMUP     -w
        return options;
    }
}