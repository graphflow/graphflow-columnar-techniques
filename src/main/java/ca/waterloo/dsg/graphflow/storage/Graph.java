package ca.waterloo.dsg.graphflow.storage;

import ca.waterloo.dsg.graphflow.storage.adjlistindex.AdjListIndexes;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.NodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.RelPropertyStore;
import lombok.Getter;
import lombok.Setter;

/**
 * Encapsulates the input data graph stored.
 */
public class Graph {

    public enum Direction {
        FORWARD((byte) 0),
        BACKWARD((byte) 1);

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

    public int getNumRels(int label) {
        return GraphCatalog.ANY == label ? (int) numRels - 1 : (int) numRelsPerLabel[label];
    }

    public int getNumNodes(int type) {
        return GraphCatalog.ANY == type ? (int) numNodes + 1 : (int) numNodesPerType[type];
    }
}
