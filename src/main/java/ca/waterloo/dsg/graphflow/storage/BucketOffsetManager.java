package ca.waterloo.dsg.graphflow.storage;

import lombok.Getter;

import java.io.Serializable;

/**
 * Each edge ID is of format edgeLabel-srcVType-scrVOff-dstVType-dstVOff-bucketOff. For each
 * (edgeLabel, srcVType) pair, a {@link BucketOffsetManager} assigns a bucketOffset to all of the
 * edges with the same label and srcVType.  The procedure to assign bucket offset is as follows.
 * Vertices are bucketed into {@link BucketOffsetManager#BUCKET_SIZE} buckets. Let k be the
 * {@link BucketOffsetManager#BUCKET_SIZE}. Vertices (for a particular srcVType, say STUDENT) with
 * vOffsets 1,...,k are bucket1, k+1,...,2k are bucket2, etc. Then all edges whose source
 * vertices fall into bucket_i get consecutive bucket offsets starting from 0,1,...
 * {@link BucketOffsetManager} keeps track of the latest bucket offset for each bucket.
 */
public class BucketOffsetManager implements Serializable {

    public static final int BUCKET_SIZE = 128;
    public static final int NUM_BUCKETS_PER_SLOT = Integer.MAX_VALUE;

    @Getter private int[][] numElementsInEachBucket;

    public BucketOffsetManager(long numVertices) {
        var numBuckets = getBucketID(numVertices) + 1;
        var numBucketSlots = (numBuckets / NUM_BUCKETS_PER_SLOT) + 1;
        numElementsInEachBucket = new int[numBucketSlots][];
        for (var i = 0; i < numBucketSlots - 1; i++) {
            numElementsInEachBucket[i] = new int[NUM_BUCKETS_PER_SLOT];
        }
        numElementsInEachBucket[numBucketSlots - 1] = new int[numBuckets % NUM_BUCKETS_PER_SLOT];
    }

    public int addElementToBucket(long bucketID) {
        var bucketSlotID = getBucketSlotID(bucketID);
        var bucketSlotOffset = getBucketSlotOffset(bucketID);
        addBucketIfNeeded(bucketSlotID, bucketSlotOffset);
        return numElementsInEachBucket[bucketSlotID][bucketSlotOffset]++;
    }

    private void addBucketIfNeeded(int bucketSlotID, int bucketSlotOffset) {
        if (bucketSlotID >= numElementsInEachBucket.length) {
            var newArray = new int[bucketSlotID][];
            System.arraycopy(numElementsInEachBucket, 0, newArray, 0, numElementsInEachBucket.length);
            numElementsInEachBucket = newArray;
        }
        if (bucketSlotOffset >= numElementsInEachBucket[bucketSlotID].length) {
            var newArray = new int[bucketSlotOffset];
            System.arraycopy(numElementsInEachBucket[bucketSlotID], 0, newArray, 0,
                numElementsInEachBucket[bucketSlotID].length);
            numElementsInEachBucket[bucketSlotID] = newArray;
        }
    }

    public static int getBucketSlotID(long bucketID) {
        return (int) (bucketID / NUM_BUCKETS_PER_SLOT);
    }

    public static int getBucketSlotOffset(long bucketID) {
        return (int) (bucketID % NUM_BUCKETS_PER_SLOT);
    }

    public static int getBucketID(long vertexId) {
        return (int) (vertexId / BUCKET_SIZE);
    }

    public static int getBucketOffset(long vertexId) {
        return (int) (vertexId % BUCKET_SIZE);
    }
}
