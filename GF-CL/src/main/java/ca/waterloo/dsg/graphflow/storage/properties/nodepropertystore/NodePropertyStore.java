package ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore;

import ca.waterloo.dsg.graphflow.storage.loader.fileiterator.LineTokensIterator;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.Column;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnBoolean;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnDouble;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnInteger;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnString;
import ca.waterloo.dsg.graphflow.util.Configuration;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import ca.waterloo.dsg.graphflow.util.container.Pair;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NodePropertyStore implements Serializable {

    private static final Logger logger = LogManager.getLogger(NodePropertyStore.class);

    @Getter private Column[][] columns;

    public void setProperties(int nodeType, long nodeOffset, LineTokensIterator tokensIterator,
        Pair<DataType[], int[]> propertyDescription) {
        var i = 1;
        while (tokensIterator.hasMoreTokens()) {
            if (!tokensIterator.isTokenEmpty()) {
                var column = getColumn(nodeType, propertyDescription.b[i]);
                if (null == column) {
                    throw new IllegalArgumentException(String.format("Cannot find the required " +
                        "column store at nodeType:%d propertyKey:%d.", nodeType,
                        propertyDescription.b[i]));
                }
                switch (propertyDescription.a[i]) {
                    case INT:
                        ((ColumnInteger) column).setProperty(nodeOffset,
                            tokensIterator.getTokenAsInteger());
                        break;
                    case DOUBLE:
                        ((ColumnDouble) column).setProperty(nodeOffset,
                            tokensIterator.getTokenAsDouble());
                        break;
                    case BOOLEAN:
                        ((ColumnBoolean) column).setProperty(nodeOffset,
                            tokensIterator.getTokenAsBoolean());
                        break;
                    case STRING:
                        ((ColumnString) column).setProperty(nodeOffset,
                            tokensIterator.getTokenAsString());
                        break;
                }
            } else {
                tokensIterator.skipToken();
            }
            i++;
        }
    }

    public void init(Pair<DataType[], int[]>[] propertyDescriptions, long[] numVerticesPerType) {
        var propertyToTypesMap = getPropertyToTypesMap(propertyDescriptions);
        columns = new Column[propertyToTypesMap.size()][];
        for (var property: propertyToTypesMap.entrySet()) {
            var types = property.getValue().b;
            var propertyKey = property.getKey();
            columns[propertyKey] = new Column[types.size()];
            var idx = 0;
            for (var type: types) {
                switch (property.getValue().a) {
                    case INT:
                        columns[propertyKey][idx++] = new ColumnInteger(type,
                            numVerticesPerType[type]);
                        break;
                    case DOUBLE:
                        columns[propertyKey][idx++] = new ColumnDouble(type,
                                numVerticesPerType[type]);
                        break;
                    case STRING:
                        columns[propertyKey][idx++] = new ColumnString(type,
                            numVerticesPerType[type]);
                        break;
                    case BOOLEAN:
                        columns[propertyKey][idx++] = new ColumnBoolean(type,
                            numVerticesPerType[type]);
                        break;
                }
            }
        }
    }

    public Column getColumn(int nodeType, int propertyKey) {
        var samePropertyColumns = this.columns[propertyKey];
        for (var column : samePropertyColumns) {
            if (column.getNodeType() == nodeType) {
                return column;
            }
        }
        return null;
    }

    private Map<Integer, Pair<DataType, List<Integer>>> getPropertyToTypesMap(Pair<DataType[],
        int[]>[] propertyDescriptions) {
        var propertyToTypeMap = new HashMap<Integer, Pair<DataType, List<Integer>>>();
        for (var type = 0; type < propertyDescriptions.length; type++) {
            var dataTypes = propertyDescriptions[type].a;
            var idxToPropertyKey = propertyDescriptions[type].b;
            for (var i = 1; i < dataTypes.length; i++) {
                var propertyKey = idxToPropertyKey[i];
                if (null == propertyToTypeMap.get(propertyKey)) {
                    propertyToTypeMap.put(propertyKey, new Pair<>(dataTypes[i], new ArrayList<>()));
                }
                propertyToTypeMap.get(propertyKey).b.add(type);
            }
        }
        return propertyToTypeMap;
    }

    @SuppressWarnings("rawtypes")
    public void serialize(String directory) throws IOException,
        InterruptedException {
        var subDirectory = directory + "/nodeStore/";
        IOUtils.mkdirs(subDirectory);
        var columnsMap = new Class[columns.length][];
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        for (var i = 0; i < columns.length; i++) {
            columnsMap[i] = new Class[columns[i].length];
            for (var j = 0; j < columns[i].length; j++) {
                columnsMap[i][j] = columns[i][j].getClass();
                int finalI = i;
                int finalJ = j;
                executor.execute(() -> {
                    try {
                        columns[finalI][finalJ].serialize(String.format("%s/%d-%d", subDirectory,
                            finalI, finalJ));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        var outputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(
            subDirectory + "main")));
        outputStream.writeObject(columnsMap);
        outputStream.close();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
    }

    @SuppressWarnings("rawtypes")
    public static NodePropertyStore deserialize(String directory) throws IOException,
        ClassNotFoundException, InterruptedException {
        var subDirectory = directory + "/nodeStore/";
        var inputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
            subDirectory + "main")));
        var columnMaps = (Class[][]) inputStream.readObject();
        var columns = new Column[columnMaps.length][];
        var executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for (var i = 0; i < columnMaps.length; i++) {
            columns[i] = new Column[columnMaps[i].length];
            for (var j = 0; j < columnMaps[i].length; j++) {
                var columnClass = columnMaps[i][j];
                var filename = String.format("%s/%d-%d", subDirectory, i, j);
                int finalI = i;
                int finalJ = j;
                executor.execute(() -> {
                    try {
                        if (ColumnInteger.class == columnClass) {
                            columns[finalI][finalJ] = ColumnInteger.deserialize(filename);
                        } else if (ColumnDouble.class == columnClass) {
                            columns[finalI][finalJ] = ColumnDouble.deserialize(filename);
                        } else if (ColumnBoolean.class == columnClass) {
                            columns[finalI][finalJ] = ColumnBoolean.deserialize(filename);
                        } else if (ColumnString.class == columnClass) {
                            columns[finalI][finalJ] = ColumnString.deserialize(filename);
                        }
                    } catch (Exception e) {
                        logger.error("Cannot deserialize a Column: " + columnClass);
                        e.printStackTrace();
                    }
                });
            }
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        var nodeStore = new NodePropertyStore();
        nodeStore.columns = columns;
        return nodeStore;
    }
}
