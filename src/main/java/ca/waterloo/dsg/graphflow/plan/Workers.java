package ca.waterloo.dsg.graphflow.plan;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanBlocking;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanBlocking.MorselDesc;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Workers {

    protected static final Logger logger = LogManager.getLogger(Workers.class);

    private final Thread[][] workers;
    private final Operator[] lastOperators;
    @Getter private double elapsedTime;
    @Getter long numOutputTuples;
    private final int numWarmupRuns;
    private final int totalNumRuns;

    public Workers(Operator lastOperator, int numThreads, int numWarmupRuns, int numActualRuns) {
        this.lastOperators = new Operator[numThreads];
        for (int i = 0; i < numThreads; i++) {
            this.lastOperators[i] = lastOperator.copy();
        }
        var morselDesc = new MorselDesc();
        for (var lastOp : lastOperators) {
            var operator = lastOp;
            while (null != operator.getPrev()) {
                operator = operator.getPrev();
            }
            if (operator instanceof ScanBlocking) {
                ((ScanBlocking) operator).setMorselDesc(morselDesc);
            }
        }
        this.numWarmupRuns = numWarmupRuns;
        this.totalNumRuns = numWarmupRuns + numActualRuns;
        workers = new Thread[totalNumRuns][numThreads];
        for (var runIdx = 0; runIdx < totalNumRuns; runIdx++) {
            for (var tid = 0; tid < numThreads; tid++) {
                workers[runIdx][tid] = new Thread(lastOperators[tid]::execute);
            }
        }
    }

    public void reset() {
        for (var lastOperator : lastOperators) {
            lastOperator.reset();
        }
    }

    public void init(Graph graph) {
        for (var lastOperator : lastOperators) {
            lastOperator.init(graph);
        }
    }

    public void execute() throws InterruptedException {
        logger.info("# threads = " + workers[0].length);
        for (var runIdx = 0; runIdx < numWarmupRuns; runIdx++) {
            reset();
            var beginTime = System.nanoTime();
            for (var tid = 0; tid < workers[0].length; tid++) {
                workers[runIdx][tid].start();
            }
            for (var tid = 0; tid < workers[0].length; tid++) {
                workers[runIdx][tid].join();
            }
            elapsedTime = IOUtils.getTimeDiff(beginTime);
            logNumOutputTuples();
            logger.info("elapsedTime: " + elapsedTime + " sec");
        }
        var totalRunTime = 0.0;
        elapsedTime = 0;
        for (var runIdx = numWarmupRuns; runIdx < totalNumRuns; runIdx++) {
            reset();
            var beginTime = System.nanoTime();
            for (var tid = 0; tid < workers[0].length; tid++) {
                workers[runIdx][tid].start();
            }
            for (var tid = 0; tid < workers[0].length; tid++) {
                workers[runIdx][tid].join();
            }
            elapsedTime = IOUtils.getTimeDiff(beginTime);
            totalRunTime += elapsedTime;
            logNumOutputTuples();
            logger.info("elapsedTime: " + elapsedTime + " sec");
        }
        logger.info("avg elapsedTime: " + (totalRunTime / ((double) totalNumRuns - numWarmupRuns)) + " sec");
    }

    private void logNumOutputTuples() {
        numOutputTuples = 0;
        for (var lastOperator : lastOperators) {
            numOutputTuples += lastOperator.getNumOutTuples();
        }
        logger.info("# out tuples: " + numOutputTuples);
    }
}
