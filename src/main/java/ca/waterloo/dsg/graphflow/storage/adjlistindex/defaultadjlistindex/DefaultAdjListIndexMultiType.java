package ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex;

import ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges.VectorAdjEdges;
import ca.waterloo.dsg.graphflow.util.Configuration;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class DefaultAdjListIndexMultiType extends DefaultAdjListIndexSingleType {

    byte[/*groupIdx*/][] bytesArrays;

    public DefaultAdjListIndexMultiType(int[][] intsArray, byte[][] bytesArrays) {
        super(intsArray);
        this.bytesArrays = bytesArrays;
    }

    public int readAdjList(VectorAdjEdges vector, long vertexOffset) {
        var bytesArray = bytesArrays[(int) vertexOffset / Configuration.getDefaultAdjListGroupingSize()];
        var intsArray =  intsArrays[(int) vertexOffset / Configuration.getDefaultAdjListGroupingSize()];
        var vertexOffsetInAGroup = (int) vertexOffset % Configuration.getDefaultAdjListGroupingSize();
        return vector.set(bytesArray, intsArray, vertexOffsetInAGroup);
    }

    protected void serializeFurther(ObjectOutputStream outputStream, int groupId)
        throws IOException {
        super.serializeFurther(outputStream, groupId);
        outputStream.writeObject(bytesArrays[groupId]);
    }

    @SuppressWarnings("rawtypes")
    public static DefaultAdjListIndexSingleType deserialize(String directory)
        throws IOException, ClassNotFoundException {
        var inputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
            directory)));
        var numGroups = inputStream.readInt();
        var intsArray = new int[numGroups][];
        var bytesArray = new byte[numGroups][];
        for (var i = 0; i < numGroups; i++) {
            intsArray[i] = (int[]) inputStream.readObject();
            bytesArray[i] = (byte[]) inputStream.readObject();
        }
        return new DefaultAdjListIndexMultiType(intsArray, bytesArray);
    }
}
