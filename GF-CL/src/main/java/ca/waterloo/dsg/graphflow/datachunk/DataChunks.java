package ca.waterloo.dsg.graphflow.datachunk;

import ca.waterloo.dsg.graphflow.datachunk.vectors.property.Vector;

import java.util.ArrayList;
import java.util.List;

public class DataChunks {

    private final PhysicalSchema schema = new PhysicalSchema();
    private final List<DataChunk> dataChunks = new ArrayList<>();

    public DataChunk getDataChunk(String variable) {
        return dataChunks.get(schema.getDataChunkPos(variable));
    }

    public int getDataChunkPos(String variable) {
        return schema.getDataChunkPos(variable);
    }

    public Vector getValueVector(String variable) {
        return dataChunks.get(schema.getDataChunkPos(variable)).
            getValueVector(schema.getVectorPos(variable));
    }

    public void addVarToPosEntry(String varName, int dataChunkPos, int vectorPos) {
        schema.addVariable(varName, dataChunkPos, vectorPos);
    }

    public DataChunk[] getUnflatDataChunks() {
        var count = 0;
        for (var pos : schema.dataChunkPosToIsFlatMap.keySet()) {
            if (!schema.dataChunkPosToIsFlatMap.get(pos)) {
                count++;
            }
        }
        var i = 0;
        var dataChunksAsLists = new DataChunk[count];
        for (var pos : schema.dataChunkPosToIsFlatMap.keySet()) {
            if (!schema.dataChunkPosToIsFlatMap.get(pos)) {
                count++;
                dataChunksAsLists[i++] = dataChunks.get(pos);
            }
        }
        return dataChunksAsLists;
    }

    public void insert(int dataChunkPos, Vector vector) {
        dataChunks.get(dataChunkPos).append(vector);
    }

    public void append(DataChunk dataChunk) {
        schema.dataChunkPosToIsFlatMap.put(dataChunks.size(), false);
        dataChunks.add(dataChunk);
    }

    public boolean isFlat(String variable) {
        return schema.isFlat(variable);
    }

    public void setAsFlat(String variable) {
        schema.setAsFlat(variable);
    }

    public void reset() {
        for (var dataChunk : dataChunks) {
            dataChunk.state.resetSelector(dataChunk.state.selectedValuesPos.length);
        }
    }

    public int size() {
        return dataChunks.size();
    }
}
