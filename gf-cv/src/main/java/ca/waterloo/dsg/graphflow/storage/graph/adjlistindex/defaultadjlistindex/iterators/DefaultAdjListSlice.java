package ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.iterators;

import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.graph.GraphCatalog;
import ca.waterloo.dsg.graphflow.util.Configuration;
import lombok.Getter;

public abstract class DefaultAdjListSlice {

    protected byte[] bytesArray;
    protected int[] intsArray;
    protected int nodeOffset;
    @Getter boolean empty;
    protected int relIdx;
    protected int relsOffsetStart, relsOffsetEnd;

    private DefaultAdjListSlice() {
    }

    public void setAdjListGroup(byte[] bytesArray, int[] intsArray, int nodeOffset) {
        this.bytesArray = bytesArray;
        this.intsArray = intsArray;
        this.nodeOffset = nodeOffset;
        empty = null == intsArray || setStartAndEndRelsOffsetsAndReturnIsEmpty();
    }

    abstract protected boolean setStartAndEndRelsOffsetsAndReturnIsEmpty();

    public DefaultAdjListSlice copy() {
        var newSlice = new DefaultAdjListUncompressedSlice();
        newSlice.bytesArray = this.bytesArray;
        newSlice.intsArray = this.intsArray;
        newSlice.nodeOffset = this.nodeOffset;
        newSlice.relIdx = this.relIdx;
        newSlice.relsOffsetStart = this.relsOffsetStart;
        newSlice.relsOffsetEnd = this.relsOffsetEnd;
        return newSlice;
    }

    public static DefaultAdjListSlice make(GraphCatalog graphCatalog, int label, Direction direction) {
        return new DefaultAdjListUncompressedSlice();
    }

    public static class DefaultAdjListUncompressedSlice extends DefaultAdjListSlice{

        public boolean isEmpty() {
            return super.isEmpty() || intsArray[nodeOffset + 1] - intsArray[nodeOffset] == 0;
        }

        protected boolean setStartAndEndRelsOffsetsAndReturnIsEmpty() {
            relIdx = Configuration.getDefaultAdjListGroupingSize() + 1;
            relsOffsetStart = intsArray[nodeOffset];
            relsOffsetEnd = intsArray[nodeOffset + 1];
            return relsOffsetEnd == relsOffsetStart;
        }
    }
}
