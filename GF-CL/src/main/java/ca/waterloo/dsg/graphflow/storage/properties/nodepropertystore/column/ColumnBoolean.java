package ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column;

import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ColumnBoolean extends Column {

    private byte[][] slots;

    private ColumnBoolean(int vertexType, byte[][] slots) {
        super(vertexType);
        this.slots = slots;
    }

    public ColumnBoolean(int vertexType, long numVertices) {
        super(vertexType, numVertices);
    }

    @Override
    protected void allocateRequiredMemory(int numSlots, int numElementsInLastSlot) {
        slots = new byte[numSlots][];
        for (var i = 0; i < numSlots; i++) {
            slots[i] = new byte[i == numSlots - 1 ? numElementsInLastSlot :
                MAX_NUM_ELEMENTS_IN_SLOT];
        }
    }

    public boolean getProperty(long vertexOffset) {
        return DataType.TRUE == slots[getSlotIdx(vertexOffset)][getSlotOffset(vertexOffset)];
    }

    public void setProperty(long vertexOffset, boolean value) {
        var slotIdx = getSlotIdx(vertexOffset);
        var slotOffset = getSlotOffset(vertexOffset);
        slots[slotIdx][slotOffset] = DataType.getBooleanValueAsByte(value);
    }

    public byte[] getPropertySlot(int vertexOffset) {
        return slots[vertexOffset / MAX_NUM_ELEMENTS_IN_SLOT];
    }

    protected void serializeFurther(ObjectOutputStream outputStream) throws IOException {
        outputStream.writeObject(slots);
    }

    public static ColumnBoolean deserialize(String directory) throws IOException,
        ClassNotFoundException {
        var inputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
            directory)));
        return new ColumnBoolean(inputStream.readInt() /*vertexType*/,
            (byte[][]) inputStream.readObject() /*slots*/);
    }
}
