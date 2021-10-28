package ca.waterloo.dsg.graphflow.plan.operator.sink;

import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;
import lombok.Getter;

import java.io.Serializable;

public class SinkCopy extends Operator implements Serializable {

    @Getter String[] QVO;

    String[] intProperties, stringProperties, nodeName;
    Vector[] intVectors, stringVectors, nodeOffsetVectors;

    int[] intValues;
    String[] stringValues;
    int[] nodeOffsets;

    public SinkCopy(String[] intProperties, String[] stringProperties, String[] nodeName,
        String[] QVO, Operator prev) {
        super(prev);
        this.intProperties = intProperties;
        this.stringProperties = stringProperties;
        this.nodeName = nodeName;
        this.QVO = QVO;
    }

    @Override
    protected void initFurther(Graph graph) {
        intValues = new int[intProperties.length];
        intVectors = new Vector[intProperties.length];
        for (var i = 0; i < intProperties.length; i++) {
            intVectors[i] = dataChunks.getValueVector(intProperties[i]);
        }

        stringValues = new String[stringProperties.length];
        stringVectors = new Vector[stringProperties.length];
        for (var i = 0; i < stringProperties.length; i++) {
            stringVectors[i] = dataChunks.getValueVector(stringProperties[i]);
        }

        nodeOffsets = new int[nodeName.length];
        nodeOffsetVectors = new Vector[nodeName.length];
        for (var i = 0; i < nodeName.length; i++) {
            nodeOffsetVectors[i] = dataChunks.getValueVector(nodeName[i]);
        }
    }

    @Override
    public void reset() {
        prev.getDataChunks().reset();
        numOutTuples = 0;
        prev.reset();
    }

    @Override
    public void processNewDataChunks() {
        for (int i = 0; i < intVectors.length; i++) {
            intValues[i] = intVectors[i].getInt(
                intVectors[i].state.getCurrSelectedValuesPos());
        }
        for (int i = 0; i < stringValues.length; i++) {
            stringValues[i] = stringVectors[i].getString(
                stringVectors[i].state.getCurrSelectedValuesPos());
        }
        for (int i = 0; i < nodeName.length; i++) {
            nodeOffsets[i] = nodeOffsetVectors[i].getNodeOffset(
                nodeOffsetVectors[i].state.getCurrSelectedValuesPos());
        }
        numOutTuples++;
    }
}
