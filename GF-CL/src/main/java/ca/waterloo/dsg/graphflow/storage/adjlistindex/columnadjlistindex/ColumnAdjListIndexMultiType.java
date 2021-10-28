package ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex;

import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

public class ColumnAdjListIndexMultiType extends ColumnAdjListIndexSingleType {

    byte[][] neighbourTypes;

    protected ColumnAdjListIndexMultiType(int vertexType, int[][] slots,
        byte[][] neighbourTypes) {
        super(vertexType, slots);
        this.neighbourTypes = neighbourTypes;
    }

    public ColumnAdjListIndexMultiType(int vertexType, long numVertices) {
        super(vertexType, numVertices);
    }

    @Override
    protected void allocateRequiredMemory(int numSlots, int numElementsInLastSlot) {
        super.allocateRequiredMemory(numSlots, numElementsInLastSlot);
        neighbourTypes = new byte[numSlots][];
        for (int i = 0; i < numSlots; i++) {
            neighbourTypes[i] = new byte[i == numSlots - 1 ? numElementsInLastSlot  :
                MAX_NUM_ELEMENTS_IN_SLOT];
            Arrays.fill(neighbourTypes[i], (byte) DataType.NULL_INTEGER);
        }
    }

    @Override
    public void setRel(long vertexOffset, int neighbourOffset, byte neighbourType) {
        super.setRel(vertexOffset, neighbourOffset);
        neighbourTypes[getSlotIdx(vertexOffset)][getSlotOffset(vertexOffset)] = neighbourType;
    }

    // Used in VOperator implementation.

    public int getNodeType(long vertexOffset) {
        return neighbourTypes[getSlotIdx(vertexOffset)][getSlotOffset(vertexOffset)];
    }

    // SerDeser

    public void serializeFurther(ObjectOutputStream outputStream) throws IOException {
        super.serializeFurther(outputStream);
        outputStream.writeObject(neighbourTypes);
    }

    public static ColumnAdjListIndexMultiType deserialize(String directory)
        throws IOException, ClassNotFoundException {
        var inputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
            directory)));
        return new ColumnAdjListIndexMultiType(inputStream.readInt() /*vertexType*/,
            (int[][]) inputStream.readObject() /*slots*/, (byte[][]) inputStream.readObject());
    }
}
