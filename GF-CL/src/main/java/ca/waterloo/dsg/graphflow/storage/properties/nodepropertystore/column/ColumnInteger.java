package ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column;

import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

public class ColumnInteger extends Column {

    protected int[][] slots;

    public ColumnInteger(int vertexType, long numVertices) {
        super(vertexType, numVertices);
    }

    protected ColumnInteger(int vertexType, int[][] slots) {
        super(vertexType);
        this.slots = slots;
    }

    @Override
    protected void allocateRequiredMemory(int numSlots, int numElementsInLastSlot) {
        slots = new int[numSlots][];
        for (var i = 0; i < numSlots; i++) {
            slots[i] = new int[i == numSlots - 1 ? numElementsInLastSlot  :
                MAX_NUM_ELEMENTS_IN_SLOT];
            Arrays.fill(slots[i], DataType.NULL_INTEGER);
        }
    }

    public int getProperty(long vertexOffset) {
        return slots[getSlotIdx(vertexOffset)][getSlotOffset(vertexOffset)];
    }

    public void setProperty(long vertexOffset, int value) {
        var slotIdx = getSlotIdx(vertexOffset);
        var slotOffset = getSlotOffset(vertexOffset);
        slots[slotIdx][slotOffset] = value;
    }

    public int[] getPropertySlot(int vertexOffset) {
        return slots[vertexOffset / MAX_NUM_ELEMENTS_IN_SLOT];
    }

    protected void serializeFurther(ObjectOutputStream outputStream) throws IOException {
        outputStream.writeObject(slots);
    }

    public static ColumnInteger deserialize(String directory) throws IOException,
        ClassNotFoundException {
        var inputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
            directory)));
        return new ColumnInteger(inputStream.readInt() /*vertexType*/,
            (int[][]) inputStream.readObject() /*slots*/);
    }
}
