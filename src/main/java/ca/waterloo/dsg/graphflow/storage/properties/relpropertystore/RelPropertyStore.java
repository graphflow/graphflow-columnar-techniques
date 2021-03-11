package ca.waterloo.dsg.graphflow.storage.properties.relpropertystore;

import ca.waterloo.dsg.graphflow.storage.BucketOffsetManager;
import ca.waterloo.dsg.graphflow.storage.loader.fileiterator.LineTokensIterator;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.relpropertylists.RelPropertyList;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.relpropertylists.RelPropertyListBoolean;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.relpropertylists.RelPropertyListDouble;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.relpropertylists.RelPropertyListInteger;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.relpropertylists.RelPropertyListString;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * {@link RelPropertyStore} stores the {\em structured} rel properties in the graph as separate
 * {\em PropertyLists}. For each (relLabel, srcVertexType, property) triple, there is a separate
 * {\em PropertyList} of a particular data type. PropertyLists are columnar-like structures in
 * that they store values for a single data type (e.g., only integers) but they are
 * adjacency list-like structures in that they are broken down into a number of buckets, one for
 * each k number of vertices. For example consider storing the "sinceDate" integer property of all
 * KNOWS rels whose source vertices are of type STUDENT. {@link RelPropertyStore} maintains one
 * {@link RelPropertyListInteger} for the "sinceDate" property of these rels. If there are KNOWS
 * rels with source nodes of type EMPLOYER, then {@link RelPropertyStore} would keep a separate
 * {@link RelPropertyListInteger} for those rels.
 *
 * Each {@link RelPropertyList} is an adjacency list-like structure. Consider an rel e_i with ID
 * relLabel-srcVType-scrVOff-dstVType-dstVOff-bucketOff, say KNOWS-STUDENT-105-EMPLOYER-1007-17
 * and storing the sinceDate property of e_i. This property is stored in a bucket srcVOff mod k
 * in the {@link RelPropertyList} corresponding to (relLabel, srcVType, sinceDate), where k is the
 * {@link BucketOffsetManager#BUCKET_SIZE}, and at offset 17 in that bucket. That is: (1) k number
 * of source vertices, v1, .., v_k are assigned to one bucket; and (2) all of the sinceDate
 * properties of rels with srcVOff is in v1,...,v_k are stored consecutively in this bucket (the
 * rels of a particular source node are not necessarily consecutive in the bucket). Recall that
 * bucketOffset is part of the ID of each rel e_i, so this bucket offset is used to locate all
 * structured properties of e_i. For example, continuing  our example, if e_i has another
 * structured property, e.g., a String "detail", we would also use 105 mod k at offset 17 in the
 * {@link RelPropertyListString} that stores the "detail" property. For the details of the bucket
 * offset assignment process, see {@link BucketOffsetManager}.
 */
public class RelPropertyStore implements Serializable {

    private static final Logger logger = LogManager.getLogger(RelPropertyStore.class);

    @Getter private RelPropertyList[][] propertyLists;

    public void setProperties(int relLabel, int srcVertexType, long srcNodeOffset, int bucketOffset,
        LineTokensIterator tokensIterator, Pair<DataType[], int[]> propertyDescription) {
        tokensIterator.skipToken();
        tokensIterator.skipToken();
        var i = 2;
        while (tokensIterator.hasMoreTokens()) {
            if (tokensIterator.isTokenEmpty()) {
                tokensIterator.skipToken();
                i++;
                continue;
            }
            var propertyList = getPropertyList(relLabel, srcVertexType, propertyDescription.b[i]);
            if (null == propertyList) {
                throw new IllegalArgumentException(String.format("No PropertyList exists for " +
                        "propertyKey:%d dataType:%s EdgeLabel:%d and srcVertexType:%d.",
                    propertyDescription.b[i], propertyDescription.a[i], relLabel, srcVertexType));
            }
            switch (propertyDescription.a[i]) {
                case INT:
                    ((RelPropertyListInteger) propertyList).setProperty(
                        srcNodeOffset, bucketOffset, tokensIterator.getTokenAsInteger());
                    break;
                case DOUBLE:
                    ((RelPropertyListDouble) propertyList).setProperty(
                        srcNodeOffset, bucketOffset, tokensIterator.getTokenAsDouble());
                    break;
                case BOOLEAN:
                    ((RelPropertyListBoolean) propertyList).setProperty(
                        srcNodeOffset, bucketOffset, tokensIterator.getTokenAsBoolean());
                    break;
                case STRING:
                    ((RelPropertyListString) propertyList).setProperty(
                        srcNodeOffset, bucketOffset, tokensIterator.getTokenAsString());
                    break;
            }
            i++;
        }
    }

    @SuppressWarnings("rawtypes")
    public void init(Pair<DataType[], int[]>[] propertyDescriptions,
        BucketOffsetManager[][] bucketOffsetManagers) {
        var propertyToLabelsMap = getPropertyToLabelsMap(propertyDescriptions);
        propertyLists = new RelPropertyList[propertyToLabelsMap.size()][];
        for (var property : propertyToLabelsMap.entrySet()) {
            var labels = property.getValue().b;
            var propertyKey = property.getKey();
            var numPropertyLists = 0;
            for (var label : labels) {
                numPropertyLists += Arrays.stream(bucketOffsetManagers[label]).filter(
                    Objects::nonNull).count();
            }
            propertyLists[propertyKey] = new RelPropertyList[numPropertyLists];
            var idx = 0;
            for (var label : labels) {
                for (var srcType = 0; srcType < bucketOffsetManagers[0].length; srcType++) {
                    if (null != bucketOffsetManagers[label][srcType]) {
                        switch (property.getValue().a) {
                            case INT:
                                propertyLists[propertyKey][idx++] = new RelPropertyListInteger(
                                    bucketOffsetManagers[label][srcType].getNumElementsInEachBucket(),
                                    label, srcType);
                                break;
                            case DOUBLE:
                                propertyLists[propertyKey][idx++] = new RelPropertyListDouble(
                                    bucketOffsetManagers[label][srcType].getNumElementsInEachBucket(),
                                    label, srcType);
                                break;
                            case STRING:
                                propertyLists[propertyKey][idx++] = new RelPropertyListString(
                                    bucketOffsetManagers[label][srcType].getNumElementsInEachBucket(),
                                    label, srcType);
                                break;
                            case BOOLEAN:
                                propertyLists[propertyKey][idx++] = new RelPropertyListBoolean(
                                    bucketOffsetManagers[label][srcType].getNumElementsInEachBucket(),
                                    label, srcType);
                                break;
                        }
                    }
                }
            }
        }
    }

    public RelPropertyList getPropertyList(int label, int nodeType, int propertyKey) {
        var propertyLists = this.propertyLists[propertyKey];
        for (var propertyList : propertyLists) {
            if (propertyList != null) {
                if (propertyList.getRelLabel() == label &&
                    propertyList.getNodeType() == nodeType) {
                    return propertyList;
                }
            }
        }
        return null;
    }

    private Map<Integer, Pair<DataType, List<Integer>>> getPropertyToLabelsMap(
        Pair<DataType[], int[]>[] propertyDescriptions) {
        var propertyToLabelsMap = new HashMap<Integer, Pair<DataType, List<Integer>>>();
        for (var label = 0; label < propertyDescriptions.length; label++) {
            var dataTypes = propertyDescriptions[label].a;
            var idxToPropertyKey = propertyDescriptions[label].b;
            for (var i = 2; i < dataTypes.length; i++) {
                var propertyKey = idxToPropertyKey[i];
                if (null == propertyToLabelsMap.get(propertyKey)) {
                    propertyToLabelsMap.put(propertyKey, new Pair<>(dataTypes[i], new ArrayList<>()));
                }
                propertyToLabelsMap.get(propertyKey).b.add(label);
            }
        }
        return propertyToLabelsMap;
    }

    public void serialize(String directory) throws IOException,
        InterruptedException {
        var subDirectory = directory + "/relStore/";
        IOUtils.mkdirs(subDirectory);
        var numPropertyLists = Arrays.stream(propertyLists).mapToInt(
            propertyLists -> propertyLists.length).toArray();
        var dataTypes = new DataType[numPropertyLists.length];
        for (var i = 0; i < numPropertyLists.length; i++) {
            if (propertyLists[i][0] instanceof RelPropertyListInteger) {
                dataTypes[i] =  DataType.INT;
            } else if (propertyLists[i][0] instanceof RelPropertyListDouble) {
                dataTypes[i] = DataType.DOUBLE;
            } else if (propertyLists[i][0] instanceof RelPropertyListBoolean) {
                dataTypes[i] =  DataType.BOOLEAN;
            } else {
                dataTypes[i] = DataType.STRING;
            }
        }
        var outputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(
            subDirectory + "main")));
        outputStream.writeObject(numPropertyLists);
        outputStream.writeObject(dataTypes);
        outputStream.close();
        var executor = Executors.newFixedThreadPool(Configuration.getMaxComputingThreads());
        for (var i = 0; i < propertyLists.length; i++) {
            for (var j = 0; j < propertyLists[i].length; j++) {
                int finalI = i;
                int finalJ = j;
                executor.execute(() -> {
                    try {
                        propertyLists[finalI][finalJ].serialize(String.format("%s/%d-%d",
                            subDirectory, finalI, finalJ));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
    }

    public static RelPropertyStore deserialize(String directory) throws IOException,
        ClassNotFoundException {
        var subDirectory = directory + "/relStore/";
        var inputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
            subDirectory + "main")));
        var numPropertyLists = (int[]) inputStream.readObject();
        var dataTypes = (DataType[]) inputStream.readObject();
        var propertyLists = new RelPropertyList[numPropertyLists.length][];
        var executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for (var i = 0; i < numPropertyLists.length; i++) {
            propertyLists[i] = new RelPropertyList[numPropertyLists[i]];
            for (var j = 0; j < numPropertyLists[i]; j++) {
                var finalJ = j;
                var finalI = i;
                executor.execute(() -> {
                    try {
                        switch (dataTypes[finalI]) {
                            case INT:
                                propertyLists[finalI][finalJ] = RelPropertyListInteger.deserialize(
                                    String.format("%s/%d-%d", subDirectory, finalI, finalJ));
                                break;
                            case DOUBLE:
                                propertyLists[finalI][finalJ] = RelPropertyListDouble.deserialize(
                                    String.format("%s/%d-%d", subDirectory, finalI, finalJ));
                                break;
                            case BOOLEAN:
                                propertyLists[finalI][finalJ] = RelPropertyListBoolean.deserialize(
                                    String.format("%s/%d-%d", subDirectory, finalI, finalJ));
                                break;
                            case STRING:
                                propertyLists[finalI][finalJ] = RelPropertyListString.deserialize(
                                    String.format("%s/%d-%d", subDirectory, finalI, finalJ));
                                break;
                        }

                    } catch (Exception e) {
                        logger.error("Cannot deserialize a PropertyList.");
                        e.printStackTrace();
                    }
                });
            }
        }
        var relStore = new RelPropertyStore();
        relStore.propertyLists = propertyLists;
        return relStore;
    }
}
