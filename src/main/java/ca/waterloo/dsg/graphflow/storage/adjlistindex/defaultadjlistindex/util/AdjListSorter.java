package ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.util;

import ca.waterloo.dsg.graphflow.storage.loader.UncompressedAdjListGroup;

public class AdjListSorter {

    int[] typesAndBucketOffsets;
    long[] offsets;

    public void sortAdjList(UncompressedAdjListGroup adjList) {
        this.typesAndBucketOffsets = adjList.getTypesAndBucketOffsets();
        this.offsets = adjList.getOffsets();
        var metadata = adjList.getMetadata();
        var startIdx = 0;
        for (var k = 0; k < metadata.length; k++) {
            sort(startIdx, metadata[k] - 1);
            startIdx = metadata[k];
        }
    }

    private void sort(int startIdx, int endIdx) {
        if (startIdx >= endIdx) {
            return;
        }
        int pivotIndex = partition(startIdx, endIdx);
        if (startIdx < pivotIndex - 1) {
            sort(startIdx, pivotIndex - 1);
        }
        if (endIdx > pivotIndex) {
            sort(pivotIndex, endIdx);
        }
    }

    private int partition(int fromIdx, int toIdx) {
        var i = fromIdx;
        var ix2 = fromIdx << 1;
        var j = toIdx;
        var jx2 = toIdx << 1;
        var pivotIdx = (fromIdx + toIdx) / 2;
        var pivotNbrType = typesAndBucketOffsets[(pivotIdx << 1)];
        var pivotNbrOffset = offsets[pivotIdx];
        var pivotBucketOffset = typesAndBucketOffsets[1 + (pivotIdx << 1)];
        while (i <= j) {
            while (typesAndBucketOffsets[ix2] < pivotNbrType ||
                (typesAndBucketOffsets[ix2] == pivotNbrType &&
                    ((offsets[i] < pivotNbrOffset) ||
                        (offsets[i] == pivotNbrOffset &&
                            typesAndBucketOffsets[1 + ix2] < pivotBucketOffset)))) {
                i++;
                ix2 += 2;
            }
            while (typesAndBucketOffsets[jx2] > pivotNbrType ||
                (typesAndBucketOffsets[jx2] == pivotNbrType &&
                    ((offsets[j] > pivotNbrOffset) ||
                        (offsets[j] == pivotNbrOffset &&
                            typesAndBucketOffsets[1 + jx2] < pivotBucketOffset)))) {
                j--;
                jx2 -= 2;
            }
            if (i <= j) {
                swapElements(i, j, ix2, jx2);
                i++;
                ix2 += 2;
                j--;
                jx2 -= 2;
            }
        }
        return i;
    }

    private void swapElements(int i, int j, int ix2, int jx2) {
        var tempNbrType = typesAndBucketOffsets[ix2];
        var tempNbrOffset = offsets[i];
        var tempBucketOffset = typesAndBucketOffsets[1 + ix2];
        typesAndBucketOffsets[ix2] = typesAndBucketOffsets[jx2];
        offsets[i] = offsets[j];
        typesAndBucketOffsets[1 + ix2] = typesAndBucketOffsets[1 + jx2];
        typesAndBucketOffsets[jx2] = tempNbrType;
        offsets[j] = tempNbrOffset;
        typesAndBucketOffsets[1 + jx2] = tempBucketOffset;
    }
}
