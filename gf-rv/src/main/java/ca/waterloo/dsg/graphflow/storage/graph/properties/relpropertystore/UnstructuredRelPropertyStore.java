package ca.waterloo.dsg.graphflow.storage.graph.properties.relpropertystore;

import ca.waterloo.dsg.graphflow.storage.graph.BucketOffsetManager;
import ca.waterloo.dsg.graphflow.storage.loader.fileiterator.LineTokensIterator;
import ca.waterloo.dsg.graphflow.storage.graph.properties.UnstructuredPropertyStore;
import ca.waterloo.dsg.graphflow.util.Configuration;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import ca.waterloo.dsg.graphflow.util.collection.ArrayUtils;
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
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class UnstructuredRelPropertyStore extends UnstructuredPropertyStore
    implements Serializable {

    private static final Logger logger = LogManager.getLogger(UnstructuredRelPropertyStore.class);

    @Getter private byte[/*label*/][/*type*/][/*b1*/][/*b2*/][/*bucketOffset*/][] bins;

    public void setProperties(int relLabel, int srcVertexType, long srcNodeOffset,
        int bucketOffset, LineTokensIterator tokensIterator, Pair<DataType[], int[]> propertyDescription) {
        var i = 2;
        var byteArray = new byte[10];
        int byteArrayPtr = 0;
        tokensIterator.skipToken();
        tokensIterator.skipToken();
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
            var bucketID = BucketOffsetManager.getBucketID(srcNodeOffset);
            bins[relLabel][srcVertexType][BucketOffsetManager.getBucketSlotID(bucketID)]
                [BucketOffsetManager.getBucketSlotOffset(bucketID)][bucketOffset] =
                Arrays.copyOf(byteArray, byteArrayPtr);
        }
    }

    @SuppressWarnings("rawtypes")
    public void init(Pair<DataType[], int[]>[] propertyDescriptions,
        BucketOffsetManager[][] bucketOffsetManagers) {
        bins = new byte[bucketOffsetManagers.length][][][][][];
        for (var i = 0; i < bucketOffsetManagers.length; i++) {
            bins[i] = new byte[bucketOffsetManagers[i].length][][][][];
            for (var j = 0; j < bucketOffsetManagers[i].length; j++) {
                if (bucketOffsetManagers[i][j] != null) {
                    var numInEachBucket = bucketOffsetManagers[i][j].getNumElementsInEachBucket();
                    bins[i][j] = new byte[numInEachBucket.length][][][];
                    for (var k = 0; k < numInEachBucket.length; k++) {
                        bins[i][j][k] = new byte[numInEachBucket[k].length][][];
                        for (var l = 0; l < numInEachBucket[k].length; l++) {
                            bins[i][j][k][l] = new byte[numInEachBucket[k][l]][];
                        }
                    }
                }
            }
        }
    }

    public int getInt(int label, int type, long offset, long bo, int key) {
        var bin = getBin(label, type, offset, bo);
        if (bin == null) {
            return DataType.NULL_INTEGER;
        }
        var ptr = searchKey(bin, key);
        if (ptr != -1) {
            return decodeInt(bin, ptr);
        }
        return DataType.NULL_INTEGER;
    }

    public double getDouble(int label, int type, long offset, long bo, int key) {
        var bin = getBin(label, type, offset, bo);
        if (bin == null) {
            return DataType.NULL_DOUBLE;
        }
        var ptr = searchKey(bin, key);
        if (ptr != -1) {
            return decodeDouble(bin, ptr);
        }
        return DataType.NULL_DOUBLE;
    }

    public String getString(int label, int type, long offset, long bo, int key) {
        var bin = getBin(label, type, offset, bo);
        if (bin == null) {
            return EMPTY_STRING;
        }
        var ptr = searchKey(bin, key);
        if (ptr != -1) {
            return decodeString(bin, ptr);
        }
        return EMPTY_STRING;
    }

    public boolean getBoolean(int label, int type, long offset, long bo, int key) {
        var bin = getBin(label, type, offset, bo);
        if (bin == null) {
            return false;
        }
        var ptr = searchKey(bin, key);
        if (ptr != -1) {
            return decodeBoolean(bin, ptr);
        }
        return false;
    }

    private byte[] getBin(int label, int type, long offset, long bo) {
        var bucketID = BucketOffsetManager.getBucketID(offset);
        var bins = this.bins[label][type];
        var bsi = BucketOffsetManager.getBucketSlotID(bucketID);
        if (bins.length <= bsi ) {
            return null;
        }
        var bso = BucketOffsetManager.getBucketSlotOffset(bucketID);
        if (bins[bsi].length <= bso) {
            return null;
        }
        return bins[bsi][bso] == null ? null : bins[bsi][bso][(int) bo];
    }

    public void serialize(String directory) throws IOException, InterruptedException {
        var subDirectory = directory + "/relStore/";
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

    public static UnstructuredRelPropertyStore deserialize(String directory) throws IOException,
        InterruptedException {
        var relStore = new UnstructuredRelPropertyStore();
        var subDirectory = directory + "/relStore/";
        var inputStream = new ObjectInputStream(new BufferedInputStream(
            new FileInputStream(subDirectory + "main")));
        var numLabels = inputStream.readInt();
        inputStream.close();
        relStore.bins = new byte[numLabels][][][][][];
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        for (var i = 0; i < relStore.bins.length; i++) {
            int finalI = i;
            executor.execute(() -> {
                try {
                    var inputS = new ObjectInputStream(new BufferedInputStream(
                        new FileInputStream(String.format("%s/%d", subDirectory, finalI))));
                    relStore.bins[finalI] = (byte[][][][][]) inputS.readObject();
                    inputS.close();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        return relStore;
    }
}
