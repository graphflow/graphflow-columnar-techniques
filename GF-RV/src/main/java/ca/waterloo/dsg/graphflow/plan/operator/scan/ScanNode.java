package ca.waterloo.dsg.graphflow.plan.operator.scan;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;

/**
 * The operator gets all edges based on a given {@link Direction} and appends each set of
 * properties of prefixes to the next operator one at a time.
 */
public class ScanNode extends Scan {

    private long[] numNodesPerType;

    public ScanNode(NodeVariable nodeVariable) {
        super(nodeVariable);
        this.operatorName = "VERTEXSCAN (" + nodeName + ")";
    }

    @Override
    public void initFurther(Graph graph) {
        this.numNodesPerType = graph.getNumNodesPerType();
        setInputAndOutputTuples();
    }

    @Override
    public void execute() {
        outputTuple.get(outNodeIdx).setNodeType(type);
        for (var i = 0; i < numNodesPerType[type]; i++) {
            outputTuple.get(outNodeIdx).setNodeOffset(i);
            numOutTuples++;
            next.processNewTuple();
        }
        notifyAllDone();
    }
}
