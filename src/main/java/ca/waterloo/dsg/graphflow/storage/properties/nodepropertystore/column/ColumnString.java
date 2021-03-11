package ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ColumnString extends Column {

    private byte[][][] slots;

    public ColumnString(int vertexType, long numVertices) {
        super(vertexType, numVertices);
    }

    protected ColumnString(int vertexType, byte[][][] slots) {
        super(vertexType);
        this.slots = slots;
    }

    @Override
    protected void allocateRequiredMemory(int numSlots, int numElementsInLastSlot) {
        slots = new byte[numSlots][][];
        for (var i = 0; i < numSlots; i++) {
            slots[i] = new byte[i == numSlots - 1 ? numElementsInLastSlot  :
                MAX_NUM_ELEMENTS_IN_SLOT][];
        }
    }

    public String getProperty(long vertexOffset) {
        var byteString = slots[getSlotIdx(vertexOffset)][getSlotOffset(vertexOffset)];
        return null == byteString ? null : new String(byteString);
    }

    public void setProperty(long vertexOffset, byte[] value) {
        var slotIdx = getSlotIdx(vertexOffset);
        var slotOffset = getSlotOffset(vertexOffset);
        slots[slotIdx][slotOffset] =  value;
    }

    @Override
    protected void serializeFurther(ObjectOutputStream outputStream) throws IOException {
        outputStream.writeObject(slots);
    }

    public static ColumnString deserialize(String directory) throws IOException,
        ClassNotFoundException {
        var inputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
            directory)));
        return new ColumnString(inputStream.readInt() /*vertexType*/,
            (byte[][][]) inputStream.readObject() /*slots*/);
    }
}
