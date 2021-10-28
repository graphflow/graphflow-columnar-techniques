package ca.waterloo.dsg.graphflow.util.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utils for Set manipulations and generations.
 */
public class SetUtils {

    /**
     * Subtracts all of the element in the given collection to subtract from the input collection.
     *
     * @param input      The input collection to subtract from.
     * @param toSubtract The collection to subtract.
     * @return The list of elements in the input collection but not in the toSubtract collection.
     */
    public static <T> List<T> subtract(Collection<T> input, Collection<T> toSubtract) {
        var result = new ArrayList<T>();
        for (T value : input) {
            var isContained = false;
            for (T otherValue : toSubtract) {
                if (value == otherValue) {
                    isContained = true;
                    break;
                }
            }
            if (!isContained) {
                result.add(value);
            }
        }
        return result;
    }

    public static List<String[]> generatePermutations(String[] arr) {
        var permutations = new ArrayList<String[]>();
        heapPermutation(arr, arr.length, permutations);
        return permutations;
    }

    private static void heapPermutation(String[] arr, int numElementsToPermute,
        List<String[]> permutations) {
        if (numElementsToPermute == 1) {
            // If size becomes 1 then prints the obtained permutation.
            var output = new String[arr.length];
            System.arraycopy(arr, 0, output, 0, arr.length);
            permutations.add(output);
        }

        for (int i = 0; i < numElementsToPermute; i++) {
            heapPermutation(arr, numElementsToPermute - 1, permutations);
            if (numElementsToPermute % 2 == 1) {
                // if size is odd, swap 0th i.e (first) and (size-1)th i.e (last) element.
                var temp = arr[0];
                arr[0] = arr[numElementsToPermute - 1];
                arr[numElementsToPermute - 1] = temp;
            } else {
                // If size is even, swap ith and (size-1)th i.e last element.
                var temp = arr[i];
                arr[i] = arr[numElementsToPermute - 1];
                arr[numElementsToPermute - 1] = temp;
            }
        }
    }
}
