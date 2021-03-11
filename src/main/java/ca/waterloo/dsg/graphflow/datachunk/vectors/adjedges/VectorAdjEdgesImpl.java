package ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges;

import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorIterator;
import ca.waterloo.dsg.graphflow.storage.BucketOffsetManager;

import java.io.Serializable;

public abstract class VectorAdjEdgesImpl extends VectorAdjEdges implements Serializable {

    @Override
    protected void setIntsArrayOffsets() {
        intsArrayOffset = relIdx + relsOffsetStart;
        intsArrayLabelEndIdx = relIdx + relsOffsetEnd;
    }

    /*********************************************************************************************
     *                         ADJACENT EDGES VECTOR IMPLEMENTATIONS                             *
     *********************************************************************************************/

    public static class VectorAdjEdgesSingleType extends VectorAdjEdgesImpl {

        protected int singleNbrType;

        public VectorAdjEdgesSingleType(int singleNbrType) {
            this.singleNbrType = singleNbrType;
        }

        @Override
        public int getNodeOffset(int pos) {
            return intsArray[intsArrayOffset + pos];
        }

        public static class VectorAdjEdgesIteratorImpl implements VectorIterator {

            VectorAdjEdgesSingleType vector;
            public int pos;

            public VectorAdjEdgesIteratorImpl(VectorAdjEdgesSingleType vector) {
                this.vector = vector;
            }

            @Override
            public void init() {
                pos = vector.intsArrayOffset;
            }

            @Override
            public int getNextNodeOffset() {
                return vector.intsArray[pos];
            }

            @Override
            public int getNextRelBucketOffset() {
                return BucketOffsetManager.getBucketOffset(vector.intsArray[pos] /* nodeOffset */);
            }

            @Override
            public void moveCursor() {
                pos++;
            }
        }

        @Override
        public VectorIterator getIterator() {
            return new VectorAdjEdgesSingleType.VectorAdjEdgesIteratorImpl(this);
        }
    }

    public static class VectorAdjEdgesSingleTypeToValBucketOffset extends VectorAdjEdgesSingleType {

        public VectorAdjEdgesSingleTypeToValBucketOffset(int singleNbrType) {
            super(singleNbrType);
        }

        @Override
        public int getRelBucketOffset(int nodeOffset) {
            return BucketOffsetManager.getBucketOffset(nodeOffset);
        }
    }

    public static class VectorAdjEdgesSingleTypeBucketOffset extends VectorAdjEdgesSingleType {

        public VectorAdjEdgesSingleTypeBucketOffset(int singleNbrType) {
            super(singleNbrType);
        }

        @Override
        public int getNodeOffset(int pos) {
            return intsArray[intsArrayOffset + pos * 2];
        }

        @Override
        public int getRelBucketOffset(int pos) {
            return intsArray[intsArrayOffset + pos * 2 + 1];
        }

        @Override
        protected void setIntsArrayOffsets() {
            intsArrayOffset = relIdx + (relsOffsetStart * 2);
            intsArrayLabelEndIdx = relIdx + (relsOffsetEnd * 2);
        }

        @Override
        public void moveIntsArrayOffset(int offsetPos) {
            intsArrayOffset += (2 * offsetPos);
        }

        public static class VectorAdjEdgesIteratorImpl implements VectorIterator {

            VectorAdjEdgesSingleTypeBucketOffset vector;
            public int pos;

            public VectorAdjEdgesIteratorImpl(VectorAdjEdgesSingleTypeBucketOffset vector) {
                this.vector = vector;
            }

            @Override
            public void init() {
                pos = vector.intsArrayOffset;
            }

            @Override
            public int getNextNodeOffset() {
                return vector.intsArray[pos];
            }

            @Override
            public int getNextRelBucketOffset() {
                return vector.intsArray[pos + 1];
            }

            @Override
            public void moveCursor() {
                pos += 2;
            }
        }

        @Override
        public VectorIterator getIterator() {
            return new VectorAdjEdgesSingleTypeBucketOffset.VectorAdjEdgesIteratorImpl(this);
        }
    }

    public static class VectorAdjEdgesMultiType extends VectorAdjEdgesImpl {

        @Override
        public int getNodeType(int pos) {
            return bytesArray[relsOffsetStart + pos];
        }

        @Override
        public int getNodeOffset(int pos) {
            return intsArray[intsArrayOffset + pos];
        }

        @Override
        public int filter(int type) {
            while (relsOffsetStart < relsOffsetEnd && bytesArray[relsOffsetStart] != type) {
                relsOffsetStart++;
            }
            var offsetEnd = relsOffsetStart;
            while (offsetEnd < relsOffsetEnd && bytesArray[offsetEnd] == type) {
                offsetEnd++;
            }
            relsOffsetEnd = offsetEnd;
            intsArrayOffset = relIdx + relsOffsetStart;
            intsArrayLabelEndIdx = relIdx + relsOffsetEnd;
            return relsOffsetEnd - relsOffsetStart;
        }

        public static class VectorAdjEdgesIteratorImpl implements VectorIterator {

            VectorAdjEdgesMultiType vector;
            public int pos;

            public VectorAdjEdgesIteratorImpl(VectorAdjEdgesMultiType vector) {
                this.vector = vector;
            }

            @Override
            public void init() {
                pos = vector.intsArrayOffset;
            }

            @Override
            public int getNextNodeOffset() {
                return vector.intsArray[pos];
            }

            @Override
            public int getNextRelBucketOffset() {
                return BucketOffsetManager.getBucketOffset(vector.intsArray[pos] /* nodeOffset */);
            }

            @Override
            public void moveCursor() {
                pos++;
            }
        }

        @Override
        public VectorIterator getIterator() {
            return new VectorAdjEdgesMultiType.VectorAdjEdgesIteratorImpl(this);
        }
    }

    public static class VectorAdjEdgesMultiTypeToValBucketOffsetNodeAndRelIterator
        extends VectorAdjEdgesMultiType {

        @Override
        public int getRelBucketOffset(int nodeOffset) {
            return BucketOffsetManager.getBucketOffset(nodeOffset);
        }
    }

    public static class VectorAdjEdgesMultiTypeBucketOffsetNodeAndRelIterator
        extends VectorAdjEdgesMultiType {

        @Override
        public int getNodeOffset(int pos) {
            return intsArray[intsArrayOffset + pos * 2];
        }

        @Override
        public int getRelBucketOffset(int pos) {
            return intsArray[intsArrayOffset + pos * 2 + 1];
        }

        @Override
        protected void setIntsArrayOffsets() {
            intsArrayOffset = relIdx + (relsOffsetStart * 2);
            intsArrayLabelEndIdx = relIdx + (relsOffsetEnd * 2);
        }

        @Override
        public int filter(int type) {
            while (intsArray[relIdx + relsOffsetStart] != type) {
                relsOffsetStart += 2;
            }
            var offsetEnd = relsOffsetStart;
            relsOffsetStart /= 2;
            while (intsArray[relIdx + offsetEnd] == type) {
                offsetEnd += 2;
            }
            relsOffsetEnd = offsetEnd;
            relsOffsetEnd /= 2;
            intsArrayOffset = relIdx + (relsOffsetStart * 2);
            return relsOffsetEnd - relsOffsetStart;
        }

        @Override
        public void moveIntsArrayOffset(int offsetPos) {
            intsArrayOffset += (2 * offsetPos);
        }

        public static class VectorAdjEdgesIteratorImpl implements VectorIterator {

            VectorAdjEdgesMultiTypeBucketOffsetNodeAndRelIterator vector;
            public int pos;

            public VectorAdjEdgesIteratorImpl(
                VectorAdjEdgesMultiTypeBucketOffsetNodeAndRelIterator vector) {
                this.vector = vector;
            }

            @Override
            public void init() {
                pos = vector.intsArrayOffset;
            }

            @Override
            public int getNextNodeOffset() {
                return vector.intsArray[pos];
            }

            @Override
            public int getNextRelBucketOffset() {
                return vector.intsArray[pos + 1];
            }

            @Override
            public void moveCursor() {
                pos += 2;
            }
        }

        @Override
        public VectorIterator getIterator() {
            return new VectorAdjEdgesMultiTypeBucketOffsetNodeAndRelIterator.
                VectorAdjEdgesIteratorImpl(this);
        }
    }
}
