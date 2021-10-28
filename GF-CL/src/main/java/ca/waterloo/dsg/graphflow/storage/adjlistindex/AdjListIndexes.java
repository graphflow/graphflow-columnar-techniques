package ca.waterloo.dsg.graphflow.storage.adjlistindex;

import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex.ColumnAdjListIndex;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex.ColumnAdjListIndexMultiType;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex.ColumnAdjListIndexSingleType;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.DefaultAdjListIndex;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.DefaultAdjListIndexMultiType;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.DefaultAdjListIndexSingleType;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.Column;
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
    @Setter ColumnAdjListIndex[/*vertexType*/][/*edgeLabel*/]
        fwdColumnAdjListIndexes, bwdColumnAdjListIndexes;

    public DefaultAdjListIndex getDefaultAdjListIndexForDirection(Direction direction,
        int vertexType, int labelIdx) {
        if (Direction.FORWARD == direction) {
            return fwdDefaultAdjListIndexes[vertexType][labelIdx];
        } else {
            return bwdDefaultAdjListIndexes[vertexType][labelIdx];
        }
    }

    public ColumnAdjListIndex getColumnAdjListIndexForDirection(Direction direction, int vertexType,
        int labelIdx) {
        if (Direction.FORWARD == direction) {
            return fwdColumnAdjListIndexes[vertexType][labelIdx];
        } else {
            return bwdColumnAdjListIndexes[vertexType][labelIdx];
        }
    }

    public static AdjListIndexes deserialize(String directory) throws InterruptedException,
        IOException, ClassNotFoundException {
        var indexes = new AdjListIndexes();
        indexes.fwdDefaultAdjListIndexes = deserializeDefaultAdjListIndexes(directory,
            Direction.FORWARD + "DefaultAdjListIndex");
        indexes.bwdDefaultAdjListIndexes = deserializeDefaultAdjListIndexes(directory,
            Direction.BACKWARD + "DefaultAdjListIndex");
        indexes.fwdColumnAdjListIndexes = deserializeColumnAdjListIndexes(directory,
            Direction.FORWARD + "ColumnAdjListIndexes");
        indexes.bwdColumnAdjListIndexes = deserializeColumnAdjListIndexes(directory,
            Direction.BACKWARD + "ColumnAdjListIndexes");
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
    public static void serializeColumnAdjListIndexes(String directory,
        ColumnAdjListIndex[][] columns, String name) throws IOException, InterruptedException {
        var subDirectory = directory + "/" + name + "/";
        IOUtils.mkdirs(subDirectory);
        var outputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(
            subDirectory + "main")));
        var columnsMap = new Class[columns.length][];
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        for (var type = 0; type < columns.length; type++) {
            int finalType = type;
            columnsMap[type] = new Class[columns[type].length];
            for (var i = 0; i < columns[type].length; i++) {
                int finalI = i;
                columnsMap[type][i] = columns[type][i].getClass();
                executor.execute(() -> {
                    try {
                        ((Column) columns[finalType][finalI]).serialize(String.format("%s/c%d-%d",
                            subDirectory, finalType, finalI));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        outputStream.writeObject(columnsMap);
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
    public static ColumnAdjListIndex[][] deserializeColumnAdjListIndexes(String directory,
        String name) throws IOException, InterruptedException, ClassNotFoundException {
        var subDirectory = directory + "/" + name + "/";
        var map = readIndexesMap(subDirectory + "main");
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        var indexes = new ColumnAdjListIndex[map.length][];
        for (var type = 0; type < indexes.length; type++) {
            var finalType = type;
            indexes[type] = new ColumnAdjListIndex[map[type].length];
            for (var i = 0; i < map[type].length; i++) {
                var finalI = i;
                var indexClass = map[type][i];
                var filename = String.format("%s/c%d-%d", subDirectory, type, i);
                executor.execute(() -> {
                    try {
                        if (indexClass.equals(
                            ColumnAdjListIndexSingleType.class)) {
                            indexes[finalType][finalI] =
                                ColumnAdjListIndexSingleType.deserialize(filename);
                        } else if (indexClass.equals(
                            ColumnAdjListIndexMultiType.class)) {
                            indexes[finalType][finalI] =
                                ColumnAdjListIndexMultiType.deserialize(filename);
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
