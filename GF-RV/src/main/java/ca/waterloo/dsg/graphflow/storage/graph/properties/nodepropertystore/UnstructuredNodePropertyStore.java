package ca.waterloo.dsg.graphflow.storage.graph.properties.nodepropertystore;

import ca.waterloo.dsg.graphflow.storage.loader.fileiterator.LineTokensIterator;
import ca.waterloo.dsg.graphflow.storage.graph.properties.UnstructuredPropertyStore;
import ca.waterloo.dsg.graphflow.util.Configuration;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import ca.waterloo.dsg.graphflow.util.collection.ArrayUtils;
import ca.waterloo.dsg.graphflow.util.container.Pair;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class UnstructuredNodePropertyStore extends UnstructuredPropertyStore
    implements Serializable {

    private byte[][][] bins;

    public void setProperties(int vertexType, long vertexOffset, LineTokensIterator tokensIterator,
        Pair<DataType[], int[]> propertyDescription) {
        var i = 1;
        var byteArray = new byte[10];
        int byteArrayPtr = 0;
        while (tokensIterator.hasMoreTokens()) {
            switch (propertyDescription.a[i]) {
                case INT:
                    if (!tokensIterator.isTokenEmpty()) {
                        var vali = tokensIterator.getTokenAsInteger();
                        byteArray = ArrayUtils.resizeIfNecessary(byteArray, byteArrayPtr + 2 + 4);
                        byteArray[byteArrayPtr++] = (byte) propertyDescription.b[i];
                        byteArray[byteArrayPtr++] = 0 /*INT*/;
                        byteArrayPtr = encodeInt(byteArray, byteArrayPtr, vali);
                    } else {
                        tokensIterator.skipToken();
                    }
                    break;
                case DOUBLE:
                    if (!tokensIterator.isTokenEmpty()) {
                        var vald = tokensIterator.getTokenAsDouble();
                        byteArray = ArrayUtils.resizeIfNecessary(byteArray, byteArrayPtr + 2 + 8);
                        byteArray[byteArrayPtr++] = (byte) propertyDescription.b[i];
                        byteArray[byteArrayPtr++] = 1 /*DOUBLE*/;
                        byteArrayPtr = encodeDouble(byteArray, byteArrayPtr, vald);
                    } else {
                        tokensIterator.skipToken();
                    }
                    break;
                case BOOLEAN:
                    if (!tokensIterator.isTokenEmpty()) {
                        var valb = tokensIterator.getTokenAsBoolean();
                        byteArray = ArrayUtils.resizeIfNecessary(byteArray, byteArrayPtr + 2 + 1);
                        byteArray[byteArrayPtr++] = (byte) propertyDescription.b[i];
                        byteArray[byteArrayPtr++] = 2 /*BOOLEAN*/;
                        byteArrayPtr = encodeBoolean(byteArray, byteArrayPtr, valb);
                    } else {
                        tokensIterator.skipToken();
                    }
                    break;
                case STRING:
                    if (!tokensIterator.isTokenEmpty()) {
                        var vals = tokensIterator.getTokenAsString();
                        byteArray = ArrayUtils.resizeIfNecessary(byteArray,
                            byteArrayPtr + 2 + vals.length + 4);
                        byteArray[byteArrayPtr++] = (byte) propertyDescription.b[i];
                        byteArray[byteArrayPtr++] = 3 /*STRING*/;
                        byteArrayPtr = encodeString(byteArray, byteArrayPtr, vals);
                    } else {
                        tokensIterator.skipToken();
                    }
                    break;
            }
            i++;
        }
        bins[vertexType][(int) vertexOffset] = Arrays.copyOf(byteArray, byteArrayPtr);
    }

    public void init(Pair<DataType[], int[]>[] propertyDescriptions, long[] numVerticesPerType) {
        bins = new byte[numVerticesPerType.length][][];
        for (var i = 0; i < numVerticesPerType.length; i++) {
            bins[i] = new byte[(int) numVerticesPerType[i]][];
        }
    }

    public void compress() {
        throw new IllegalArgumentException();
    }

    public int getInt(int type, long offset, int key) {
        var bin = bins[type][(int) offset];
        var ptr = searchKey(bin, key);
        if (ptr != -1) {
            return decodeInt(bin, ptr);
        }
        return DataType.NULL_INTEGER;
    }

    public double getDouble(int type, long offset, int key) {
        var bin = bins[type][(int) offset];
        var ptr = searchKey(bin, key);
        if (ptr != -1) {
            return decodeDouble(bin, ptr);
        }
        return DataType.NULL_INTEGER;
    }

    public String getString(int type, long offset, int key) {
        var bin = bins[type][(int) offset];
        var ptr = searchKey(bin, key);
        if (ptr != -1) {
            return decodeString(bin, ptr);
        }
        return EMPTY_STRING;
    }

    public boolean getBoolean(int type, long offset, int key) {
        var bin = bins[type][(int) offset];
        var ptr = searchKey(bin, key);
        if (ptr != -1) {
            return decodeBoolean(bin, ptr);
        }
        return false;
    }

    @SuppressWarnings("rawtypes")
    public void serialize(String directory) throws IOException, InterruptedException {
        var subDirectory = directory + "/nodeStore/";
        IOUtils.mkdirs(subDirectory);
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        for (var i = 0; i < bins.length; i++) {
            int finalI = i;
            executor.execute(() -> {
                try {
                    var outputStream = new ObjectOutputStream(new BufferedOutputStream(
                        new FileOutputStream(String.format("%s/%d", subDirectory, finalI))));
                    outputStream.writeObject(bins[finalI]);
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        var outputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(
            subDirectory + "main")));
        outputStream.writeInt(bins.length);
        outputStream.close();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
    }

    @SuppressWarnings("unchecked")
    public static UnstructuredNodePropertyStore deserialize(String directory) throws IOException,
        InterruptedException {
        var nodeStore = new UnstructuredNodePropertyStore();
        var subDirectory = directory + "/nodeStore/";
        var inputStream = new ObjectInputStream(new BufferedInputStream(
            new FileInputStream(subDirectory + "main")));
        var numTypes = inputStream.readInt();
        inputStream.close();
        nodeStore.bins = new byte[numTypes][][];
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        for (var i = 0; i < nodeStore.bins.length; i++) {
            int finalI = i;
            executor.execute(() -> {
                try {
                    var inputS = new ObjectInputStream(new BufferedInputStream(
                        new FileInputStream(String.format("%s/%d", subDirectory, finalI))));
                    nodeStore.bins[finalI] = (byte[][]) inputS.readObject();
                    inputS.close();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        return nodeStore;
    }
}
