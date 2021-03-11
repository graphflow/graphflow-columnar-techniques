package ca.waterloo.dsg.graphflow.storage.loader;

import ca.waterloo.dsg.graphflow.util.Configuration;
import lombok.Getter;

/**
 * This class stores the incoming edges to an AdjList, before serialization. A number of
 * restrictions are applied to the incoming edges which should be relaxed in the future:
 *  1. Size of the neighbourOffset should not be more than that of an {@link Integer}.
 *  2. Size of the neighbourType should be one byte.
 * */
public class UncompressedAdjListGroup {

    @Getter private int numRels;
    @Getter private int[] metadata;
    @Getter private int[] typesAndBucketOffsets;
    @Getter private long[] offsets;

    public UncompressedAdjListGroup() {
        metadata = new int[Configuration.getDefaultAdjListGroupingSize()];
    }

    public void init() {
        numRels = 0;
        for (var i = 0; i < Configuration.getDefaultAdjListGroupingSize(); i++) {
            var count = metadata[i];
            metadata[i] = numRels;
            numRels += count;
        }
        if (null == offsets || numRels > offsets.length) {
            typesAndBucketOffsets = new int[numRels << 1];
            offsets = new long[numRels];
        }
    }

    public void incrementCount(int vertexOffset) {
        metadata[vertexOffset]++;
    }

    public void addRel(int vertexOffset, int type, long offset, int bucketOffset) {
        if (offset > Integer.MAX_VALUE) {
            throw new IllegalStateException("[AdjList encoding]: numNeighbourTypeBytes > 1.");
        }
        if (type > Byte.MAX_VALUE) {
            throw new IllegalStateException("[AdjList encoding]: highestNbrOffset > INT_MAX");
        }
        addRelAtOffset(type, offset, bucketOffset, metadata[vertexOffset]++);
    }

    private void addRelAtOffset(int type, long offset, int bucketOffset, int pos) {
        offsets[pos] = offset;
        var posSquared = pos << 1;
        typesAndBucketOffsets[posSquared] = type;
        typesAndBucketOffsets[posSquared + 1] = bucketOffset;
    }
}
