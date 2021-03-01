package ca.waterloo.dsg.graphflow.plan.operator.extend;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.defaultadjlist.DefaultAdjListIndex;
import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.iterators.DefaultAdjListSlice;
import ca.waterloo.dsg.graphflow.tuple.Schema;

public abstract class ExtendDefaultAdjList extends Extend {

    DefaultAdjListIndex defaultAdjListIndex;
    DefaultAdjListSlice defaultAdjListSlice;
    int labelIdx;

    public ExtendDefaultAdjList(AdjListDescriptor ald, Schema inSchema) {
        super(ald, inSchema);
    }

    @Override
    public void initFurther(Graph graph) {
        setInputOutputTuples();
        defaultAdjListSlice = DefaultAdjListSlice.make(graph.getGraphCatalog(),
            ald.getRelVariable().getLabel(), ald.getDirection());
        labelIdx = graph.getGraphCatalog()
            .getTypeToDefaultAdjListIndexLabelsMapInDirection(ald.getDirection())
            .get(ald.getBoundNodeVariable().getType())
            .indexOf(ald.getRelVariable().getLabel());
        defaultAdjListIndex = graph.getAdjListIndexes().getDefaultAdjListIndexForDirection(
            ald.getDirection(), ald.getBoundNodeVariable().getType(), labelIdx);
    }

    @Override
    public void processNewTuple() {
        defaultAdjListIndex.fillAdjList(defaultAdjListSlice, fromNodeVal.getNodeOffset());
        if (!defaultAdjListSlice.isEmpty()) {
            extend();
        }
    }

    abstract protected void extend();
}
