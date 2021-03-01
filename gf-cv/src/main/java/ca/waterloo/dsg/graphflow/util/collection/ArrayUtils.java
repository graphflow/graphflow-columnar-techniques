package ca.waterloo.dsg.graphflow.util.collection;

import java.util.Arrays;

/**
 * Utility methods for resize operations on arrays.
 */
public class ArrayUtils {

    private static final double RESIZE_MULTIPLIER = 1.2;

    /**
     * Checks if the capacity of {@code array} is at least {@code minCapacity} and increases the
     * capacity by a factor of {@link ArrayUtils#RESIZE_MULTIPLIER} if it isn't.
     *
     * @param array The array to resize if necessary.
     * @param minCapacity The minimum required getSize of the array.
     */
    public static int[] resizeIfNecessary(int[] array, int minCapacity) {
        return (minCapacity > array.length) ? Arrays.copyOf(array, ArrayUtils.getNewCapacity(
            array.length, minCapacity)) : array;
    }

    public static byte[] resizeIfNecessary(byte[] array, int minCapacity) {
        return (minCapacity > array.length) ? Arrays.copyOf(array, ArrayUtils.getNewCapacity(
            array.length, minCapacity)) : array;
    }

    public static long[] resizeIfNecessary(long[] array, int minCapacity) {
        return (minCapacity > array.length) ? Arrays.copyOf(array, ArrayUtils.getNewCapacity(
            array.length, minCapacity)) : array;
    }

    public static double[] resizeIfNecessary(double[] array, int minCapacity) {
        return (minCapacity > array.length) ? Arrays.copyOf(array, ArrayUtils.getNewCapacity(
            array.length, minCapacity)) : array;
    }

    public static boolean[] resizeIfNecessary(boolean[] array, int minCapacity) {
        return (minCapacity > array.length) ? Arrays.copyOf(array, ArrayUtils.getNewCapacity(
            array.length, minCapacity)) : array;
    }

    public static <T> T[] resizeIfNecessary(T[] array, int minCapacity) {
        return (minCapacity > array.length) ? Arrays.copyOf(array, ArrayUtils.getNewCapacity(
            array.length, minCapacity)) : array;
    }

    public static long[][] create2DLongArray(long capacity) {
        int buckets = (int) (capacity / Integer.MAX_VALUE) + 1;
        var array = new long[buckets][];
        for (var i = 0;i < buckets - 1;i++) {
            array[i] = new long[Integer.MAX_VALUE];
        }
        array[buckets - 1] = new long[(int) capacity % Integer.MAX_VALUE];
        return array;
    }

    public static int[][] create2DIntegerArray(long capacity) {
        int buckets = (int) (capacity / Integer.MAX_VALUE) + 1;
        var array = new int[buckets][];
        for (var i = 0;i < buckets - 1;i++) {
            array[i] = new int[Integer.MAX_VALUE];
        }
        array[buckets - 1] = new int[(int) capacity % Integer.MAX_VALUE];
        return array;
    }

    private static int getNewCapacity(int oldCapacity, int minCapacity) {
        var newCapacity = (int) (oldCapacity * RESIZE_MULTIPLIER) + 1;
        // Check if {@code newCapacity} > {@code minCapacity} before returning.
        return (newCapacity > minCapacity) ? newCapacity : minCapacity;
    }

}
