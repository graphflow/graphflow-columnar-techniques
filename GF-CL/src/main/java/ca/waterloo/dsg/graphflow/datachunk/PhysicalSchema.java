package ca.waterloo.dsg.graphflow.datachunk;

import ca.waterloo.dsg.graphflow.util.container.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Schema of {@link DataChunks} i.e., intermediate relation passed between operators.
 */
public class PhysicalSchema implements Serializable {

    public final Map<Integer, Boolean> dataChunkPosToIsFlatMap = new HashMap<>();
    private final Map<Integer, Integer> dataChunkPosToNumVectors = new HashMap<>();
    private final Map<String, Pair<Integer, Integer>> varToDataChunkAndVectorPosMap = new HashMap<>();

    public void addVariable(String varName, int dataChunkPos, int vectorPos) {
        varToDataChunkAndVectorPosMap.put(varName, new Pair<>(dataChunkPos, vectorPos));
        dataChunkPosToNumVectors.putIfAbsent(dataChunkPos, 0);
        dataChunkPosToNumVectors.put(dataChunkPos, dataChunkPosToNumVectors.get(dataChunkPos) + 1);
    }

    public boolean hasVariable(String propertyName) {
        return varToDataChunkAndVectorPosMap.containsKey(propertyName);
    }

    public String getAnyVariable(int dataChunkPos) {
        for (var variable : varToDataChunkAndVectorPosMap.keySet()) {
            if (varToDataChunkAndVectorPosMap.get(variable).a == dataChunkPos) {
                return variable;
            }
        }
        return null;
    }

    public int getDataChunkPos(String varName) {
        return varToDataChunkAndVectorPosMap.get(varName).a;
    }

    public int getVectorPos(String varName) {
        return varToDataChunkAndVectorPosMap.get(varName).b;
    }

    public int getNumVectors(int dataChunkPos) {
        return dataChunkPosToNumVectors.get(dataChunkPos);
    }

    public boolean isFlat(String variable) {
        return dataChunkPosToIsFlatMap.get(getDataChunkPos(variable));
    }

    public void setAsFlat(String variable) {
        dataChunkPosToIsFlatMap.put(getDataChunkPos(variable), true);
    }

    public PhysicalSchema clone() {
        var schema = new PhysicalSchema();
        for (var variable : varToDataChunkAndVectorPosMap.keySet()) {
            var pair = varToDataChunkAndVectorPosMap.get(variable);
            schema.addVariable(variable, pair.a, pair.b);
        }
        for (var dataChunkPos : dataChunkPosToIsFlatMap.keySet()) {
            schema.dataChunkPosToIsFlatMap.put(dataChunkPos,
                dataChunkPosToIsFlatMap.get(dataChunkPos));
        }
        for (var dataChunkPos : dataChunkPosToNumVectors.keySet()) {
            schema.dataChunkPosToNumVectors.put(dataChunkPos,
                dataChunkPosToNumVectors.get(dataChunkPos));
        }
        return schema;
    }
}
