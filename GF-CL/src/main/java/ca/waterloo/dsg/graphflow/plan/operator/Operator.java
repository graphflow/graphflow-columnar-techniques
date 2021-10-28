package ca.waterloo.dsg.graphflow.plan.operator;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.storage.Graph;
import lombok.Getter;

import java.io.Serializable;

public abstract class Operator implements Serializable {

    @Getter protected String operatorName;
    protected Operator next;
    protected Operator prev;

    @Getter protected DataChunks dataChunks;

    @Getter protected long numOutTuples = 0;

    public Operator(Operator prev) {
        if (prev != null) {
            prev.next = this;
            this.prev = prev;
        }
    }

    public void init(Graph graph) {
        if (null != prev) {
            prev.init(graph);
            this.dataChunks = prev.dataChunks;
        }
        initFurther(graph);
    }

    protected abstract void initFurther(Graph graph);

    public abstract void processNewDataChunks();

    public void notifyAllDone() {
        if (null != next) {
            next.notifyAllDone();
        }
    }

    public void execute() {
        if (null != prev) {
            prev.execute();
        }
    }

    public void reset() {
        if (prev != null) {
            prev.reset();
        }
    }
}
