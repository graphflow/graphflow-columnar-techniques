package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.plan.RegularQueryPlan;
import ca.waterloo.dsg.graphflow.plan.operator.extend.Extend;
import ca.waterloo.dsg.graphflow.plan.operator.intersect.IntersectLL;
import ca.waterloo.dsg.graphflow.plan.operator.scan.Scan;
import ca.waterloo.dsg.graphflow.storage.graph.BucketOffsetManager;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore.UnstructuredNodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.graph.properties.relpropertystore.UnstructuredRelPropertyStore;
import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.AdjListIndexes;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Stack;
import java.util.StringJoiner;

public class AbstractPlanRunner extends AbstractRunner {

    protected static final Logger logger = LogManager.getLogger(AbstractPlanRunner.class);

    protected static class QueryExecutionStat {

        Query query;
        String plan;
        double execTime;
        long numTuples;
        String numFactorizedOps;
        int i;

        @Override
        public String toString() {
            var joiner = new StringJoiner("\n");
            joiner.add(String.format("cypher query   : %s", query.qstr));
            joiner.add(String.format("query name     : %s", query.name));
            joiner.add(String.format("plan           : %s", plan.substring(0, plan.length() - 1)));
            joiner.add(String.format("i              : %s", i));
            joiner.add(String.format("# fact ops     : %s", numFactorizedOps));
            joiner.add(String.format("execution time : %.3f ms", execTime));
//            joiner.add(String.format("mem usage      : %.3f MB", size));
            joiner.add(String.format("#tuples        : %d", numTuples));
            return joiner.toString();
        }
    }

    protected static Graph graph;

    public static void loadDataset(String directory, List<Double> stats) throws IOException,
        ClassNotFoundException, InterruptedException {
        logger.info("Loading dataset...");
        graph = new Graph();
        triggerGC();
        var memPointA = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        logger.info("Deserializing the Nodes store...");
        long timePoint = System.nanoTime();
        graph.setNodePropertyStore(UnstructuredNodePropertyStore.deserialize(directory));
        stats.add(IOUtils.getTimeDiff(timePoint));
        triggerGC();
        var memPointB = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        stats.add(getMemoryInMB(memPointA, memPointB));
        logger.info("Deserializing the Rel store...");
        timePoint = System.nanoTime();
        graph.setRelPropertyStore(UnstructuredRelPropertyStore.deserialize(directory));
        stats.add(IOUtils.getTimeDiff(timePoint));
        triggerGC();
        var memPointC = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        stats.add(getMemoryInMB(memPointB, memPointC));
        var indexes = new AdjListIndexes();
        logger.info("Deserializing the Fwd AdjListsIndexes...");
        timePoint = System.nanoTime();
        indexes.setFwdDefaultAdjListIndexes(AdjListIndexes.deserializeDefaultAdjListIndexes(
            directory, Direction.FORWARD + "DefaultAdjListIndexes"));
        stats.add(IOUtils.getTimeDiff(timePoint));
        triggerGC();
        var memPointD = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        stats.add(getMemoryInMB(memPointC, memPointD));
        logger.info("Deserializing the Bwd AdjListsIndexes...");
        timePoint = System.nanoTime();
        indexes.setBwdDefaultAdjListIndexes(AdjListIndexes.deserializeDefaultAdjListIndexes(
            directory, Direction.BACKWARD + "DefaultAdjListIndexes"));
        stats.add(IOUtils.getTimeDiff(timePoint));
        triggerGC();
        var memPointE = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        stats.add(getMemoryInMB(memPointD, memPointE));
        graph.setAdjListIndexes(indexes);
        logger.info("Deserializing rest of the Graph data...");
        timePoint = System.nanoTime();
        graph.setNumNodes((long) IOUtils.deserializeObject(directory + "numNodes"));
        graph.setNumNodesPerType((long[]) IOUtils.deserializeObject(directory + "numNodesPerType"));
        graph.setNumRels((long) IOUtils.deserializeObject(directory + "numRels"));
        graph.setNumRelsPerLabel((long[]) IOUtils.deserializeObject(directory + "numRelsPerLabel"));
        graph.setBucketOffsetManagers((BucketOffsetManager[][]) IOUtils.deserializeObject(
            directory + "bucketOffsetManagers"));
        graph.setGraphCatalog((GraphCatalog) IOUtils.deserializeObject(directory + "graphCatalog"));
        stats.add(IOUtils.getTimeDiff(timePoint));
        triggerGC();
        var memPointF = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        stats.add(getMemoryInMB(memPointE, memPointF));
//        graph.getGraphCatalog().print();
    }

    private static void triggerGC() throws InterruptedException {
        System.gc();
        Thread.sleep(1000);
    }

    private static double getMemoryInMB(long memBefore, long memAfter) {
        var memUsageDiff = memAfter == -1 ? memBefore : memAfter - memBefore;
        memUsageDiff = memUsageDiff < 0 ? 0 : memUsageDiff;
        return memUsageDiff / (1024.0 * 1024.0);
    }

    public static void executeASingleQueryPlan(QueryExecutionStat queryExecutionStat,
        RegularQueryPlan plan, int warmUpRuns, int runs) throws InterruptedException {
//        logger.info(String.format("Benchmark query: %s", queryExecutionStat.query.query));
        logger.info(String.format("Plan: %s", plan));
        System.out.println("short extend info = " + getShortExtendInfo(plan));
        plan.init(graph);
        queryExecutionStat.plan = plan.toString();
        for (var i = 0; i < warmUpRuns; i++) {
            logger.info(String.format("Warm-up run: %d/%d", i + 1, warmUpRuns));
            plan.execute();
            triggerGC();
            logger.info(String.format("\tElapsed time\t\t: %.3f", plan.getElapsedTimeMillis()));
        }
        var aggregateExecTime = 0.0;
        for (var i = 0; i < runs; i++) {
            plan.init(graph);
            logger.info(String.format("Run %d/%d", i + 1, runs));
            triggerGC();
            plan.execute();
            triggerGC();
            logger.info(String.format("\tElapsed time\t\t: %.3f", plan.getElapsedTimeMillis()));
            aggregateExecTime += plan.getElapsedTimeMillis();
        }
        queryExecutionStat.numTuples = plan.getNumOutputTuples();
        queryExecutionStat.execTime = aggregateExecTime / runs;
    }

    protected static String getShortExtendInfo(RegularQueryPlan rqp) {
        var o = rqp.getLastOperator();
        var s = new Stack<String>();
        while (o != null) {
            if (o instanceof Extend) {
                var ald = ((Extend) o).getAld();
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
            } else if (o instanceof IntersectLL) {
                var alds = ((IntersectLL) o).getAlds();
                var str = "";
                for (var i = 0; i < 2; i++) {
                    str += " (" + alds[i].getBoundNodeVariable().getVariableName() + ")*";
                    if (alds[i].getDirection() == Direction.FORWARD) {
                        str += "->";
                    } else {
                        str += "<-";
                    }
                    str += "(" + alds[i].getToNodeVariable().getVariableName() + ")";
                }
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
}