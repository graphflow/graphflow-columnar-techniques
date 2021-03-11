package ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges;

import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.util.Configuration;

public abstract class VectorAdjEdges extends Vector {

    protected byte[] bytesArray;
    protected int[] intsArray;
    protected int nodeOffset;
    protected int relIdx;
    protected int relsOffsetStart, relsOffsetEnd;

    protected int intsArrayOffset;
    protected int intsArrayLabelEndIdx;

    @Override
    public int set(byte[] bytesArray, int[] intsArray, int nodeOffset) {
        if (intsArray == null) {
            return 0;
        }
        this.bytesArray = bytesArray;
        this.intsArray = intsArray;
        this.nodeOffset = nodeOffset;
        relIdx = Configuration.getDefaultAdjListGroupingSize() + 1;
        relsOffsetStart = intsArray[nodeOffset];
        relsOffsetEnd = intsArray[nodeOffset + 1];
        setIntsArrayOffsets();
        return relsOffsetEnd - relsOffsetStart;
    }

    @Override
    public int getIntsArrayOffset() {
        return intsArrayOffset;
    }

    public void moveIntsArrayOffset(int offsetPos) {
        intsArrayOffset += offsetPos;
    }

    protected abstract void setIntsArrayOffsets();
}
