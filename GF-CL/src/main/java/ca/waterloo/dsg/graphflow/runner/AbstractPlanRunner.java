package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.BucketOffsetManager;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.AdjListIndexes;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.NodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.RelPropertyStore;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.StringJoiner;

public class AbstractPlanRunner extends AbstractRunner {

    protected static final Logger logger = LogManager.getLogger(AbstractPlanRunner.class);

    protected static class QueryExecutionStat {

        Query query;
        String plan;
        double execTime;
        long numTuples;

        @Override
        public String toString() {
            var joiner = new StringJoiner("\n");
            joiner.add(String.format("query name     : %s", query.name));
            joiner.add(String.format("cypher query   : %s", query.qstr));
            joiner.add(String.format("QVO            : %s", query.printQVO()));
            joiner.add(String.format("execution time : %.3f ms", execTime));
            joiner.add(String.format("#tuples        : %d", numTuples));
            return joiner.toString();
        }
    }

    protected static Graph graph;

    public static void loadDataset(String dir) throws IOException, ClassNotFoundException, InterruptedException {
        graph = new Graph();
        logger.info("Deserializing the Node store...");
        graph.setNodePropertyStore(NodePropertyStore.deserialize(dir));
        logger.info("Deserializing the Rel store...");
        graph.setRelPropertyStore(RelPropertyStore.deserialize(dir));
        var indexes = new AdjListIndexes();
        logger.info("Deserializing the FWD default adjacency lists...");
        indexes.setFwdDefaultAdjListIndexes(AdjListIndexes.deserializeDefaultAdjListIndexes(dir,
                Graph.Direction.FORWARD + "DefaultAdjListIndexes"));
        logger.info("Deserializing the FWD columnar adjacency lists...");
        indexes.setFwdColumnAdjListIndexes(AdjListIndexes.deserializeColumnAdjListIndexes(dir,
                Graph.Direction.FORWARD + "ColumnAdjListIndexes"));
        logger.info("Deserializing the BWD default adjacency lists...");
        indexes.setBwdDefaultAdjListIndexes(AdjListIndexes.deserializeDefaultAdjListIndexes(dir,
                Graph.Direction.BACKWARD + "DefaultAdjListIndexes"));
        logger.info("Deserializing the BWD columnar adjacency lists...");
        indexes.setBwdColumnAdjListIndexes(AdjListIndexes.deserializeColumnAdjListIndexes(dir,
                Graph.Direction.BACKWARD + "ColumnAdjListIndexes"));
        graph.setAdjListIndexes(indexes);
        logger.info("Deserializing rest of the Graph data...");
        graph.setNumNodes((long) IOUtils.deserializeObject(dir + "numNodes"));
        graph.setNumNodesPerType((long[]) IOUtils.deserializeObject(dir + "numNodesPerType"));
        graph.setNumRels((long) IOUtils.deserializeObject(dir + "numRels"));
        graph.setNumRelsPerLabel((long[]) IOUtils.deserializeObject(dir + "numRelsPerLabel"));
        graph.setBucketOffsetManagers((BucketOffsetManager[][]) IOUtils.deserializeObject(
                dir + "bucketOffsetManagers"));
        graph.setGraphCatalog((GraphCatalog) IOUtils.deserializeObject(dir + "graphCatalog"));
        triggerGC();
        logger.info("Done.");
    }

    private static void triggerGC() throws InterruptedException {
        System.gc();
        Thread.sleep(100);
    }


    public static void executeASingleQueryPlan(QueryExecutionStat queryExecutionStat, Operator plan,
                                               int warmUpRuns, int runs) throws InterruptedException {
        plan.init(graph);
        for (var i = 0; i < warmUpRuns; i++) {
            plan.reset();
            logger.info(String.format("Warm-up run: %d/%d", i + 1, warmUpRuns));
            var beginTime = System.nanoTime();
            plan.execute();
            var elapsedTime = IOUtils.getTimeDiff(beginTime);
            triggerGC();
            logger.info(String.format("\tElapsed time\t\t: %.3f", elapsedTime));
        }
        var aggregateExecTime = 0.0;
        for (var i = 0; i < runs; i++) {
            plan.reset();
            plan.init(graph);
            logger.info(String.format("Run %d/%d", i + 1, runs));
            triggerGC();
            var beginTime = System.nanoTime();
            plan.execute();
            var elapsedTime = IOUtils.getTimeDiff(beginTime);
            triggerGC();
            logger.info(String.format("\tElapsed time\t\t: %.3f", elapsedTime));
            aggregateExecTime += elapsedTime;
        }
        queryExecutionStat.numTuples = plan.getNumOutTuples();
        queryExecutionStat.execTime = aggregateExecTime / runs;
    }
}