package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.parser.QueryParser;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.plan.Enumerator;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.runner.utils.ArgsFactory;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

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
                3 /* default number of runs*/;
        warmUpRuns = cmdLine.hasOption(ArgsFactory.WARM_UP_RUNS) ?
                Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.WARM_UP_RUNS)) :
                2 /* default number of warmUp runs */;
        loadDataset(inputDir);
        for (var i = 0; i < 10; i++) {
            Thread.sleep(100);
        }
        var queryExecutionStat = new QueryExecutionStat();
        var plan = resolvePlan(cmdLine.getOptionValue(ArgsFactory.BENCHMARK_QUERY), queryExecutionStat);

        /*execute*/
        executeASingleQueryPlan(queryExecutionStat, plan, warmUpRuns, runs);

        /*print statistics*/
        System.out.println(queryExecutionStat);

        System.exit(0);
    }

    private static Operator resolvePlan(String queryFilename, QueryExecutionStat qes) throws IOException {

        /* read query */
        if (!(new File(queryFilename)).exists()) {
            logger.info(queryFilename);
            throw new FileNotFoundException("Query file is not found.");
        }
        qes.query = new Gson().fromJson(new JsonReader(new StringReader(
                Files.readString(Paths.get(queryFilename), StandardCharsets.US_ASCII))), Query.class);

        /* get query plan */
        var enumerator = new Enumerator((RegularQuery) QueryParser.parseQuery(qes.query.qstr    ,
                graph.getGraphCatalog()), graph);
        return enumerator.generatePlan(qes.query.qvo);
    }

    private static Options getCommandLineOptions() {
        var options = new Options();
        options.addOption(ArgsFactory.getBenchmarkQueryOption());              // BENCHMARK              -q
        options.addOption(ArgsFactory.getInputGraphDirectoryOption());    // INPUT_DIR              -i
        options.addOption(ArgsFactory.getNumberRunsOptions());            // NUM_RUNS               -r
        options.addOption(ArgsFactory.getWarmupRunsOption());             // WARMUP_RUNS            -w
        return options;
    }
}
