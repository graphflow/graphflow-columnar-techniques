package ca.waterloo.dsg.graphflow.util.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utils for Set manipulations and generations.
 */
public class SetUtils {

    /**
     * Subtracts all of the element in the given collection to subtract from the input collection.
     *
     * @param input The input collection to subtract from.
     * @param toSubtract The collection to subtract.
     * @return The list of elements in the input collection but not in the toSubtract collection.
     */
    public static <T> List<T> subtract(Collection<T> input, Collection<T> toSubtract) {
        var result = new ArrayList<T>();
        for (T value : input) {
            if (!toSubtract.contains(value)) {
                result.add(value);
            }
        }
        return result;
    }

    public static <T> List<T> merge(Collection<T> inputA, T inputB) {
        var result = new HashSet<T>();
        if (null != inputA) {
            result.addAll(inputA);
        }
        if (null != inputB) {
            result.add(inputB);
        }
        return new ArrayList<>(result);
    }

    public static <T> Set<T> intersect(Collection<T> inputA, Collection<T> inputB) {
        Set<T> intersection = new HashSet<T>(inputA);
        intersection.retainAll(inputB);
        return intersection;
    }
}
