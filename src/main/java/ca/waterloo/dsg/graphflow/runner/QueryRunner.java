package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.parser.QueryParser;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.plan.Enumerator;
import ca.waterloo.dsg.graphflow.plan.Workers;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.extend.ExtendAdjLists;
import ca.waterloo.dsg.graphflow.plan.operator.extend.ExtendColumn;
import ca.waterloo.dsg.graphflow.plan.operator.flatten.Flatten;
import ca.waterloo.dsg.graphflow.plan.operator.scan.Scan;
import ca.waterloo.dsg.graphflow.plan.operator.sink.SinkCopy;
import ca.waterloo.dsg.graphflow.runner.utils.ArgsFactory;
import ca.waterloo.dsg.graphflow.storage.BucketOffsetManager;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.AdjListIndexes;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.NodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.RelPropertyStore;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Stack;

public class QueryRunner extends AbstractRunner {

    protected static final Logger logger = LogManager.getLogger(QueryRunner.class);

    protected static Graph graph;
    public static int NUM_WARMUP_RUNS = 2;
    public static int NUM_ACTUAL_RUNS = 3;
    public static int NUM_QUERIES = 33;

    public static void main(String[] args) throws Exception {
        var cmdLine = parseCommandLine(args, getCommandLineOptions());
        if (null == cmdLine) {
            return;
        }
        loadDataset(sanitizeDirStr(cmdLine.getOptionValue(ArgsFactory.INPUT_DIR)));
        for (var i = 0; i < 100; i++) { // wait and trigger gc. 10 sec wait.
            triggerGC();
        }
        for (var i = 0; i < 10; i++) {
            Thread.sleep(100);
        }

        var threads = Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID));

        // IC01.
        logger.info(LDBCQueries.queries[0].name);
        var enumerator = new Enumerator((RegularQuery) QueryParser.parseQuery(
            LDBCQueries.queries[0].str, graph.getGraphCatalog()), graph);
        var plan = enumerator.generatePlan(LDBCQueries.queries[0].QVO);
        var workers16 = new Workers(plan, threads, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        workers16.init(graph);
        workers16.execute();

        // IC02.
        logger.info(LDBCQueries.queries[1].name);
        var enumerator2 = new Enumerator((RegularQuery) QueryParser.parseQuery(
            LDBCQueries.queries[1].str, graph.getGraphCatalog()), graph);
        var plan2 = enumerator2.generatePlan(LDBCQueries.queries[1].QVO);
        var workers16_2 = new Workers(plan2, threads, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        workers16_2.init(graph);
        workers16_2.execute();

        // IC03.
        logger.info(LDBCQueries.queries[2].name);
        var enumerator3 = new Enumerator((RegularQuery) QueryParser.parseQuery(
            LDBCQueries.queries[2].str, graph.getGraphCatalog()), graph);
        var plan3 = enumerator3.generatePlan(LDBCQueries.queries[2].QVO);
        var workers16_3 = new Workers(plan3, threads, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        workers16_3.init(graph);
        workers16_3.execute();

        // IC04.
        logger.info(LDBCQueries.queries[0].name);
        var enumerator4 = new Enumerator((RegularQuery) QueryParser.parseQuery(
            LDBCQueries.queries[0].str, graph.getGraphCatalog()), graph);
        var plan4 = enumerator4.generatePlan(LDBCQueries.queries[0].QVO);/*
        var op = plan4.getPrev().getPrev();
        while (!(op instanceof Scan)) {
            // remove flattens.
            if (op instanceof Flatten) {
                var opNext = op.getNext();
                var opPrev = op.getPrev();
                opNext.setPrev(opPrev);
                opPrev.setNext(opNext);
            }
            op = op.getPrev();
        }
        op = plan4;
        while (!(op instanceof ExtendColumn)) {
            op = op.getPrev();
        }
        var opNext = op.getNext();
        var opPrev = op.getPrev();
        opNext.setPrev(opPrev);
        opPrev.setNext(opNext);*/
        logger.info("short extend info = " + getShortExtendInfo(plan4));
        var workers8_4 = new Workers(plan4, threads, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        workers8_4.init(graph);
        workers8_4.execute();

        // IC05.
        logger.info(LDBCQueries.queries[4].name);
        var enumerator5 = new Enumerator((RegularQuery) QueryParser.parseQuery(
            LDBCQueries.queries[4].str, graph.getGraphCatalog()), graph);
        var plan5 = enumerator5.generatePlan(LDBCQueries.queries[4].QVO);
        logger.info("short extend info = " + getShortExtendInfo(plan5));
        var workers16_5 = new Workers(plan5, threads, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        workers16_5.init(graph);
        workers16_5.execute();

        // IC06.
        logger.info(LDBCQueries.queries[5].name);
        var enumerator6 = new Enumerator((RegularQuery) QueryParser.parseQuery(
            LDBCQueries.queries[5].str, graph.getGraphCatalog()), graph);
        var plan6 = enumerator6.generatePlan(LDBCQueries.queries[5].QVO);
        var workers16_6 = new Workers(plan6, threads, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        workers16_6.init(graph);
        workers16_6.execute();

        // IC07.
        /*logger.info("IC07");
        var enumerator7 = new Enumerator((RegularQuery) QueryParser.parseQuery(
            LDBCQueries.queries[6].str, graph.getGraphCatalog()), graph);
        var plan7 = enumerator7.generatePlan(LDBCQueries.queries[6].QVO);
        var workers16_7 = new Workers(plan7, threads, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        workers16_7.init(graph);
        workers16_7.execute();

        // IC08.
        logger.info(LDBCQueries.queries[7].name);
        var enumerator8 = new Enumerator((RegularQuery) QueryParser.parseQuery(
            LDBCQueries.queries[7].str, graph.getGraphCatalog()), graph);
        var plan8 = enumerator8.generatePlan(LDBCQueries.queries[7].QVO);
        var workers16_8 = new Workers(plan8, threads, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        workers16_8.init(graph);
        workers16_8.execute();

        // IC09.
        logger.info(LDBCQueries.queries[8].name);
        var enumerator9 = new Enumerator((RegularQuery) QueryParser.parseQuery(
            LDBCQueries.queries[8].str, graph.getGraphCatalog()), graph);
        var plan9 = enumerator9.generatePlan(LDBCQueries.queries[8].QVO);
        var workers16_9 = new Workers(plan9, threads, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        workers16_9.init(graph);
        workers16_9.execute();

        // IC11.
        logger.info(LDBCQueries.queries[11].name);
        var enumerator11 = new Enumerator((RegularQuery) QueryParser.parseQuery(
            LDBCQueries.queries[11].str, graph.getGraphCatalog()), graph);
        var plan11 = enumerator11.generatePlan(LDBCQueries.queries[9].QVO);
        var workers16_11 = new Workers(plan11, threads, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        workers16_11.init(graph);
        workers16_11.execute();

        // IC12.
        logger.info(LDBCQueries.queries[12].name);
        var enumerator12 = new Enumerator((RegularQuery) QueryParser.parseQuery(
            LDBCQueries.queries[12].str, graph.getGraphCatalog()), graph);
        var plan12 = enumerator12.generatePlan(LDBCQueries.queries[10].QVO);
        plan12.init(graph);
        plan12.execute();
        //var workers16_12 = new Workers(plan12, threads, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        //workers16_12.init(graph);
        //workers16_12.execute();*/

        /* var enumerator = new Enumerator((RegularQuery) QueryParser.parseQuery(
            "MATCH (t:title)-[hmi1:has_movie_info]->(mi1:movie_info)," +
            "      (t:title)-[hmi2:has_movie_info_idx]->(mi2:movie_info_idx)," +
            "      (t:title)-[hk:movie_keyword]->(k:keyword)," +
            "      (t:title)-[hcs:cast_info]->(n:name)," +
            "      (t:title)-[hci:has_complete_cast]->(ci:complete_cast) " +
            "WHERE ci.subject_id <= 2 and ci.status_id = 4 and mi1.info_type_id = 3 and" +
            "      mi2.info_type_id = 100 and k.keyword = 'murder' and mi1.info = 'Horror' and" +
            "      n.gender = 'm' and t.production_year > 2000 " +
            "RETURN t", graph.getGraphCatalog()), graph);
        var ordering = new String[] { "n", "t", "mi1", "k", "ci", "mi2" };
        var plan = enumerator.generatePlan(ordering);
        plan.init(graph);
        for (var i = 0; i < NUM_WARMUP_RUNS; i++) {
            plan.reset();
            plan.execute();
        }
        logger.info(Arrays.toString(((SinkCopy) plan).getQVO()) +
            ", # out tuples: " + plan.getNumOutTuples());
        for (var i = 0; i < NUM_ACTUAL_RUNS; i++) {
            plan.reset();
            var startTime = System.nanoTime();
            plan.execute();
            var elapsed_time = IOUtils.getTimeDiff(startTime);
            logger.info(elapsed_time);
        }

        var workers16 = new Workers(plan, 16, NUM_WARMUP_RUNS, NUM_ACTUAL_RUNS);
        workers16.init(graph);
        workers16.execute(); */

        /* List<Operator>[] plansPerQuery = new List[NUM_QUERIES];
        var times = new StringBuilder(100);
        var QVOStr = new StringBuilder(100);
        var spaceStr = new StringBuilder(100);
        for (var QID = 0; QID < NUM_QUERIES; QID++) {
            if (JobsQueries.queries[QID].equals("")) {
                continue;
            }
            logger.info("Running Q" + (QID + 1) + ".");
            var enumerator = new Enumerator((RegularQuery) QueryParser.parseQuery(
                JobsQueries.queries[QID], graph.getGraphCatalog()), graph);
            plansPerQuery[QID] = enumerator.generatePlans();
            for (var j = 0; j < plansPerQuery[QID].size(); j++) {
                var lastOperator = plansPerQuery[QID].get(j);
                // Warmup run.
                lastOperator.init(graph);
                lastOperator.execute();

                QVOStr.setLength(0);
                QVOStr.append("QVO: ");
                QVOStr.append(Arrays.toString(((SinkCopy) lastOperator).getQVO()));
                logger.info(QVOStr.toString() + ", # out tuples: " + lastOperator.getNumOutTuples());
                spaceStr.setLength(0);
                spaceStr.append(" ".repeat(Math.max(0, QVOStr.length() + 16)));

                lastOperator.reset();
                lastOperator.execute();
                times.setLength(0);
                for (var i = 0; i < NUM_ACTUAL_RUNS; i++) {
                    lastOperator.reset();
                    var startTime = System.nanoTime();
                    lastOperator.execute();
                    var elapsed_time = IOUtils.getTimeDiff(startTime);
                    logger.info(spaceStr.toString() + lastOperator.getNumOutTuples());
                    times.append(i < NUM_ACTUAL_RUNS - 1 ? String.format("%.5f,", elapsed_time) :
                        String.format("%.5f", elapsed_time));
                }
                logger.info(times.toString());
            }
        } */
        System.exit(0);
    }

    public static void executeWarmup(Workers workers) throws InterruptedException {
        workers.init(graph);
        workers.execute();
        logger.info("# out tuples: " + workers.getNumOutputTuples());
    }

    public static void execute(Workers workers) throws InterruptedException {
        workers.init(graph);
        workers.execute();
        logger.info(workers.getElapsedTime());
    }

    public static void loadDataset(String dir) throws IOException,
        ClassNotFoundException, InterruptedException {
        graph = new Graph();
        triggerGC();
        graph.setNodePropertyStore(NodePropertyStore.deserialize(dir));
        graph.setRelPropertyStore(RelPropertyStore.deserialize(dir));
        triggerGC();
        var indexes = new AdjListIndexes();
        indexes.setFwdDefaultAdjListIndexes(AdjListIndexes.deserializeDefaultAdjListIndexes(dir,
            Direction.FORWARD + "DefaultAdjListIndexes"));
        indexes.setFwdColumnAdjListIndexes(AdjListIndexes.deserializeColumnAdjListIndexes(dir,
            Direction.FORWARD + "ColumnAdjListIndexes"));
        triggerGC();
        indexes.setBwdDefaultAdjListIndexes(AdjListIndexes.deserializeDefaultAdjListIndexes(dir,
            Direction.BACKWARD + "DefaultAdjListIndexes"));
        indexes.setBwdColumnAdjListIndexes(AdjListIndexes.deserializeColumnAdjListIndexes(dir,
            Direction.BACKWARD + "ColumnAdjListIndexes"));
        triggerGC();
        graph.setAdjListIndexes(indexes);
        graph.setNumNodes((long) IOUtils.deserializeObject(dir + "numNodes"));
        graph.setNumNodesPerType((long[]) IOUtils.deserializeObject(dir + "numNodesPerType"));
        graph.setNumRels((long) IOUtils.deserializeObject(dir + "numRels"));
        graph.setNumRelsPerLabel((long[]) IOUtils.deserializeObject(dir + "numRelsPerLabel"));
        graph.setBucketOffsetManagers((BucketOffsetManager[][]) IOUtils.deserializeObject(
            dir + "bucketOffsetManagers"));
        graph.setGraphCatalog((GraphCatalog) IOUtils.deserializeObject(dir + "graphCatalog"));
        triggerGC();
    }

    private static void triggerGC() throws InterruptedException {
        System.gc();
        Thread.sleep(100);
    }

    protected static String getShortExtendInfo(Operator o) {
        var s = new Stack<String>();
        while (o != null) {
            if (o instanceof ExtendColumn || o instanceof ExtendAdjLists) {
                var ald = (o instanceof ExtendColumn) ?
                    ((ExtendColumn) o).getALD() : ((ExtendAdjLists) o).getALD();
                var str = "(" + ald.getBoundNodeVariable().getVariableName() + ")*";
                if (ald.getDirection() == Direction.FORWARD) {
                    str += "->";
                } else {
                    str += "<-";
                }
                str += "(" + ald.getToNodeVariable().getVariableName() + ")";
                s.push(str);
            } else if (o instanceof Scan) {
                var scan = (Scan) o;
                var str = "(" + scan.getNodeName() + ")";
                s.push(str);
            }
            o = o.getPrev();
        }
        StringBuilder str = new StringBuilder(s.pop());
        while (!s.empty()) {
            str.append(", ").append(s.pop());
        }
        return str.toString();
    }


    private static Options getCommandLineOptions() {
        var options = new Options();
        options.addOption(ArgsFactory.getInputGraphDirectoryOption());    // INPUT_DIR          -i
        options.addOption(ArgsFactory.getCID());                          // CID                -c
        return options;
    }
}
