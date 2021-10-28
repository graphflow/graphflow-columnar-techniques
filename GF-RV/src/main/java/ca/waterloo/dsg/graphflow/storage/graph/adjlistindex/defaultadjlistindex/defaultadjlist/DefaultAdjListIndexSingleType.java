package ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.defaultadjlist;

import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.iterators.DefaultAdjListSlice;
import ca.waterloo.dsg.graphflow.util.Configuration;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class DefaultAdjListIndexSingleType extends DefaultAdjListIndex {

    int[/*groupIdx*/][] intsArrays;

    protected int getNumGroups() {
        return intsArrays.length;
    }

    public DefaultAdjListIndexSingleType(int[][] intsArray) {
        this.intsArrays = intsArray;
    }

    public void fillAdjList(DefaultAdjListSlice slice, long vertexOffset) {
        var intsArray =  intsArrays[(int) vertexOffset / Configuration.getDefaultAdjListGroupingSize()];
        var vertexOffsetInAGroup = (int) vertexOffset % Configuration.getDefaultAdjListGroupingSize();
        slice.setAdjListGroup(null /*bytes array*/, intsArray, vertexOffsetInAGroup);
    }

    protected void serializeFurther(ObjectOutputStream outputStream, int groupId)
        throws IOException {
        outputStream.writeObject(intsArrays[groupId]);
    }

    @SuppressWarnings("rawtypes")
    public static DefaultAdjListIndexSingleType deserialize(String directory)
        throws IOException, ClassNotFoundException {
        var inputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(directory)));
        var numGroups = inputStream.readInt();
        var intsArray = new int[numGroups][];
        for (var i = 0; i < numGroups; i++) {
            intsArray[i] = (int[]) inputStream.readObject();
        }
        return new DefaultAdjListIndexSingleType(intsArray);
    }
}
