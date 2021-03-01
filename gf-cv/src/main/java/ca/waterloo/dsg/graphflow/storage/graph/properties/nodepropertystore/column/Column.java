package ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore.column;

import lombok.Getter;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * A Column stores attribute values of vertices a given vertexType. The entire column is divided
 * into fixed-sized slots, wherein the maximum size of each slot is {@link Short#MAX_VALUE}.
 */
public abstract class Column implements Serializable {

    protected static final int MAX_NUM_ELEMENTS_IN_SLOT = Short.MAX_VALUE;

    @Getter protected int nodeType;

    protected Column(int nodeType) {
        this.nodeType = nodeType;
    }

    protected Column(int vertexType, long numVertices) {
        this(vertexType);
        var numSlots = (int) ((numVertices / MAX_NUM_ELEMENTS_IN_SLOT) + 1);
        var numElementsInLastSlot = (int) (numVertices % MAX_NUM_ELEMENTS_IN_SLOT);
        allocateRequiredMemory(numSlots, numElementsInLastSlot);
    }

    protected abstract void allocateRequiredMemory(int numSlots, int numElementsInLastSlot);

    public static int getSlotIdx(long vertexOffset) {
        return (int) (vertexOffset / MAX_NUM_ELEMENTS_IN_SLOT);
    }

    public static int getSlotOffset(long vertexOffset) {
        return (int) (vertexOffset % MAX_NUM_ELEMENTS_IN_SLOT);
    }

    public final void serialize(String directory) throws IOException {
        var outputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(
            directory)));
        outputStream.writeInt(nodeType);
        serializeFurther(outputStream);
        outputStream.close();
    }

    protected abstract void serializeFurther(ObjectOutputStream outputStream) throws IOException;
}
