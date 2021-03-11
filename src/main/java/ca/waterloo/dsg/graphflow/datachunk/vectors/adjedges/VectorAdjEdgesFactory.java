package ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges;

import ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges.VectorAdjEdgesImpl.VectorAdjEdgesMultiType;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges.VectorAdjEdgesImpl.VectorAdjEdgesMultiTypeBucketOffsetNodeAndRelIterator;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges.VectorAdjEdgesImpl.VectorAdjEdgesMultiTypeToValBucketOffsetNodeAndRelIterator;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges.VectorAdjEdgesImpl.VectorAdjEdgesSingleType;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges.VectorAdjEdgesImpl.VectorAdjEdgesSingleTypeBucketOffset;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges.VectorAdjEdgesImpl.VectorAdjEdgesSingleTypeToValBucketOffset;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;

public class VectorAdjEdgesFactory {

    public static VectorAdjEdgesImpl make(GraphCatalog catalog, int label, Direction direction) {
        var nbrTypes = catalog.getLabelToNbrTypeMapInDirection(direction).get(label);
        if (!catalog.labelHasProperties(label)) {
            return 1 == nbrTypes.size() ?
                new VectorAdjEdgesSingleType(nbrTypes.get(0)) :
                new VectorAdjEdgesMultiType();
        } else if (!catalog.labelDirectionHasMultiplicityOne(label, Direction.FORWARD) &&
            !catalog.labelDirectionHasMultiplicityOne(label, Direction.BACKWARD)) {
            return 1 == nbrTypes.size() ?
                new VectorAdjEdgesSingleTypeBucketOffset(nbrTypes.get(0)) :
                new VectorAdjEdgesMultiTypeBucketOffsetNodeAndRelIterator();
        } else {
            return 1 == nbrTypes.size() ?
                new VectorAdjEdgesSingleTypeToValBucketOffset(nbrTypes.get(0)) :
                new VectorAdjEdgesMultiTypeToValBucketOffsetNodeAndRelIterator();
        }
    }
}
