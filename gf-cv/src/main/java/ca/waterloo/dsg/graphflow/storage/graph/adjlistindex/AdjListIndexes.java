package ca.waterloo.dsg.graphflow.storage.graph.adjlistindex;

import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.defaultadjlist.DefaultAdjListIndex;
import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.defaultadjlist.DefaultAdjListIndexMultiType;
import ca.waterloo.dsg.graphflow.storage.graph.adjlistindex.defaultadjlistindex.defaultadjlist.DefaultAdjListIndexSingleType;
import ca.waterloo.dsg.graphflow.util.Configuration;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import lombok.Setter;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AdjListIndexes {

    @Setter DefaultAdjListIndex[/*vertexType*/][/*edgeLabel*/]
        fwdDefaultAdjListIndexes, bwdDefaultAdjListIndexes;

    public DefaultAdjListIndex getDefaultAdjListIndexForDirection(Direction direction,
        int vertexType, int labelIdx) {
        if (Direction.FORWARD == direction) {
            return fwdDefaultAdjListIndexes[vertexType][labelIdx];
        } else {
            return bwdDefaultAdjListIndexes[vertexType][labelIdx];
        }
    }

    public static AdjListIndexes deserialize(String directory) throws InterruptedException,
        IOException, ClassNotFoundException {
        var indexes = new AdjListIndexes();
        indexes.fwdDefaultAdjListIndexes = deserializeDefaultAdjListIndexes(directory,
            Direction.FORWARD + "DefaultAdjListIndex");
        indexes.bwdDefaultAdjListIndexes = deserializeDefaultAdjListIndexes(directory,
            Direction.BACKWARD + "DefaultAdjListIndex");
        return indexes;
    }

    @SuppressWarnings("rawtypes")
    public static void serializeDefaultAdjListIndexes(String directory,
        DefaultAdjListIndex[][] indexes, String name) throws IOException, InterruptedException {
        var subDirectory = directory + "/" + name + "/";
        IOUtils.mkdirs(subDirectory);
        var outputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(
            subDirectory + "main")));
        var defaultAdjListIndexMap = new Class[indexes.length][];
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        for (var type = 0; type < indexes.length; type++) {
            var numTypes = null == indexes[type] ? 0 : indexes[type].length;
            defaultAdjListIndexMap[type] = new Class[numTypes];
            int finalType = type;
            for (var i = 0; i < numTypes; i++) {
                int finalI = i;
                defaultAdjListIndexMap[type][i] = indexes[type][i].getClass();
                executor.execute(() -> {
                    try {
                        indexes[finalType][finalI].serialize(String.format("%s/d%d-%d",
                            subDirectory, finalType, finalI));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        outputStream.writeObject(defaultAdjListIndexMap);
        outputStream.close();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
    }

    @SuppressWarnings("rawtypes")
    public static DefaultAdjListIndex[][] deserializeDefaultAdjListIndexes(String directory,
        String name) throws IOException, InterruptedException, ClassNotFoundException {
        var subDirectory = directory + "/" + name + "/";
        var map = readIndexesMap(subDirectory + "main");
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        var indexes = new DefaultAdjListIndex[map.length][];
        for (var type = 0; type < indexes.length; type++) {
            var finalType = type;
            indexes[type] = new DefaultAdjListIndex[map[type].length];
            for (var i = 0; i < map[type].length; i++) {
                var finalI = i;
                var indexClass = map[type][i];
                var filename = String.format("%s/d%d-%d", subDirectory, type, i);
                executor.execute(() -> {
                    try {
                        if (indexClass.equals(DefaultAdjListIndexSingleType.class)) {
                            indexes[finalType][finalI] = DefaultAdjListIndexSingleType.deserialize(
                                filename);
                        } else if (indexClass.equals(DefaultAdjListIndexMultiType.class)) {
                            indexes[finalType][finalI] = DefaultAdjListIndexMultiType.deserialize(
                                filename);
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        return indexes;
    }

    @SuppressWarnings("rawtypes")
    private static Class[][] readIndexesMap(String file) throws IOException, ClassNotFoundException {
        var inputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
        var map = (Class[][]) inputStream.readObject();
        inputStream.close();
        return map;
    }
}
