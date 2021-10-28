package ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column;

import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

public class ColumnDouble extends Column {

    private double[][] slots;

    public ColumnDouble(int vertexType, long numVertices) {
        super(vertexType, numVertices);
    }

    protected ColumnDouble(int vertexType, double[][] slots) {
        super(vertexType);
        this.slots = slots;
    }

    @Override
    protected void allocateRequiredMemory(int numSlots, int numElementsInLastSlot) {
        slots = new double[numSlots][];
        for (var i = 0; i < numSlots; i++) {
            slots[i] = new double[i == numSlots - 1 ? numElementsInLastSlot  :
                MAX_NUM_ELEMENTS_IN_SLOT];
            Arrays.fill(slots[i], DataType.NULL_DOUBLE);
        }
    }

    public double getProperty(long vertexOffset) {
        return slots[getSlotIdx(vertexOffset)][getSlotOffset(vertexOffset)];
    }

    public void setProperty(long vertexOffset, double value) {
        var slotIdx = getSlotIdx(vertexOffset);
        var slotOffset = getSlotOffset(vertexOffset);
        slots[slotIdx][slotOffset] = value;
    }

    public double[] getPropertySlot(int vertexOffset) {
        return slots[vertexOffset / MAX_NUM_ELEMENTS_IN_SLOT];
    }

    protected void serializeFurther(ObjectOutputStream outputStream) throws IOException {
        outputStream.writeObject(slots);
    }

    public static ColumnDouble deserialize(String directory) throws IOException,
        ClassNotFoundException {
        var inputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
            directory)));
        return new ColumnDouble(inputStream.readInt() /*vertexType*/,
            (double[][]) inputStream.readObject() /*slots*/);
    }
}
