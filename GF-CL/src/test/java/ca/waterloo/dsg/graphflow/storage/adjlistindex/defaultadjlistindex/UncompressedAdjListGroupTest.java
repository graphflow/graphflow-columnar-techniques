package ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex;

import ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.util.AdjListSorter;
import ca.waterloo.dsg.graphflow.storage.loader.UncompressedAdjListGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class UncompressedAdjListGroupTest {

    protected UncompressedAdjListGroup createUncompressedAdjList() {
        var adjList = new UncompressedAdjListGroup();
        adjList.incrementCount(0);
        adjList.incrementCount(0);
        adjList.incrementCount(0);
        adjList.incrementCount(1);
        adjList.incrementCount(1);
        adjList.incrementCount(1);
        adjList.incrementCount(1);
        adjList.incrementCount(1);
        adjList.init();
        adjList.addRel(0, 0, 1, 3);
        adjList.addRel(0, 0, 3, 2);
        adjList.addRel(0, 1, 2, 1);
        adjList.addRel(1, 0, 1, 4);
        adjList.addRel(1, 0, 4, 5);
        adjList.addRel(1, 0, 3, 3);
        adjList.addRel(1, 1, 2, 2);
        adjList.addRel(1, 1, 5, 1);
        return adjList;
    }

    @Test
    public void componentArraysTest() {
        var uncompressedAdjList = createUncompressedAdjList();
        var expectedMetadata = new int[] {3, 8};
        var actualMetadata = Arrays.copyOf(uncompressedAdjList.getMetadata(), 2);
        Assertions.assertArrayEquals(expectedMetadata, actualMetadata);
        Assertions.assertArrayEquals(new int[]{0, 3, 0, 2, 1, 1, 0, 4, 0, 5, 0, 3, 1, 2, 1, 1},
            uncompressedAdjList.getTypesAndBucketOffsets());
        Assertions.assertArrayEquals(new long[]{1, 3, 2, 1, 4, 3, 2, 5},
            uncompressedAdjList.getOffsets());
    }

    @Test
    public void uncompressedAdjListSorterTest() {
        var uncompressedAdjList = createUncompressedAdjList();
        var sorter = new AdjListSorter();
        sorter.sortAdjList(uncompressedAdjList);
        var expectedMetadata = new int[] {3, 8};
        var actualMetadata = Arrays.copyOf(uncompressedAdjList.getMetadata(), 2);
        Assertions.assertArrayEquals(expectedMetadata, actualMetadata);
        Assertions.assertArrayEquals(new int[]{0, 3, 0, 2, 1, 1, 0, 4, 0, 3, 0, 5, 1, 2, 1, 1},
            uncompressedAdjList.getTypesAndBucketOffsets());
        Assertions.assertArrayEquals(new long[]{1, 3, 2, 1, 3, 4, 2, 5},
            uncompressedAdjList.getOffsets());
    }
}
