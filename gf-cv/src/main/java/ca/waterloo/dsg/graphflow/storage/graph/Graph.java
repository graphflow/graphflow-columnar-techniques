package ca.waterloo.dsg.graphflow.storage.graph;

import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.AdjListIndexes;
import ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore.NodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.graph.properties.relpropertystore.RelPropertyStore;
import lombok.Getter;
import lombok.Setter;

/**
 * Encapsulates the input data graph stored.
 */
public class Graph {

    public enum Direction {
        FORWARD         ((byte) 0),
        BACKWARD        ((byte) 1);

        public byte byteValue;

        Direction(byte DirectionAsByte) {
            this.byteValue = DirectionAsByte;
        }
    }

    // Default adjacency lists containing the neighbour node IDs sorted by ID.
    @Getter @Setter private AdjListIndexes adjListIndexes;
    // Graph metadata.
    @Getter @Setter private long numNodes;
    @Getter @Setter private long[] numNodesPerType;
    @Getter @Setter private long numRels;
    @Getter @Setter private long[] numRelsPerLabel;
    // type, label, property stores
    @Getter @Setter private GraphCatalog graphCatalog;
    @Getter @Setter private NodePropertyStore nodePropertyStore;
    @Getter @Setter private RelPropertyStore relPropertyStore;
    @Getter @Setter private BucketOffsetManager[][] bucketOffsetManagers;

    public long getNumRels(int label) {
        return GraphCatalog.ANY == label ? numRels - 1 : numRelsPerLabel[label];
    }

    public long getNumNodes(int type) {
        return GraphCatalog.ANY == type ? numNodes + 1 : numNodesPerType[type];
    }
}
