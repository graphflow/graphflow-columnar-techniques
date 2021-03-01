package ca.waterloo.dsg.graphflow.storage.graph;

import ca.waterloo.dsg.graphflow.runner.utils.DatasetMetadata;
import ca.waterloo.dsg.graphflow.storage.graph.Graph.Direction;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphCatalog implements Serializable {

    public enum Cardinality {
        ONE_TO_ONE      ("1-1"),
        ONE_TO_MANY     ("1-n"),
        MANY_TO_ONE     ("n-1"),
        MANY_TO_MANY    ("n-n");

        public String val;

        Cardinality(String val) {
            this.val = val;
        }
    }

    public final static int ANY = -1;

    @Getter private int nextNodePropertyKey = 0;
    @Getter private int nextRelPropertyKey = 0;
    private Map<String, Integer> stringToIntegerTypeKeyMap = new HashMap<>();
    private Map<String, Integer> stringToIntegerLabelKeyMap = new HashMap<>();
    private Map<String, Integer> stringToIntegerNodePropertyKeyMap = new HashMap<>();
    private Map<String, Integer> stringToIntegerRelPropertyKeyMap = new HashMap<>();
    private Map<Integer, DataType> nodePropertyKeyToDataTypeMap = new HashMap<>();
    private Map<Integer, DataType> relPropertyKeyToDataTypeMap = new HashMap<>();

    @Getter private List<Boolean[]> labelStoreCompressed = new ArrayList<>();
    private List<Cardinality> labelToCardinalityMap = new ArrayList<>();
    private List<Integer> labelToNumPropertiesMap = new ArrayList<>();
    private List<List<Integer>> labelToTypesFwdMap = new ArrayList<>();
    private List<List<Integer>> labelToTypesBwdMap = new ArrayList<>();
    private List<List<Integer>> typeToDefaultAdjListIndexLabelsFwdMap = new ArrayList<>();
    private List<List<Integer>> typeToDefaultAdjListIndexLabelsBwdMap = new ArrayList<>();

    public void init(DatasetMetadata metadata) {
        for (var i = 0; i < metadata.nodeFileDescriptions.length; i++) {
            var vertexFile = metadata.nodeFileDescriptions[i];
            stringToIntegerTypeKeyMap.put(vertexFile.getType(), i);
            typeToDefaultAdjListIndexLabelsFwdMap.add(new ArrayList<>());
            typeToDefaultAdjListIndexLabelsBwdMap.add(new ArrayList<>());
        }
        for (var i = 0; i < metadata.relFileDescriptions.length; i++) {
            var edgeFile = metadata.relFileDescriptions[i];
            stringToIntegerLabelKeyMap.put(edgeFile.getLabel(), i);
            labelToCardinalityMap.add(edgeFile.getCardinality());
            labelStoreCompressed.add(edgeFile.getStoreCompressed());
            labelToTypesFwdMap.add(new ArrayList<>());
            labelToTypesBwdMap.add(new ArrayList<>());
            labelToNumPropertiesMap.add(null);
        }
    }

    public int getNumTypes() {
        return stringToIntegerTypeKeyMap.size();
    }

    public int getNumLabels() {
        return stringToIntegerLabelKeyMap.size();
    }

    public int getTypeKey(String type) {
        if (null == stringToIntegerTypeKeyMap.get(type)) {
            throw new IllegalArgumentException("Type " + type + " does not exist in the database.");
        }
        return stringToIntegerTypeKeyMap.get(type);
    }

    public int getLabelKey(String label) {
        if (null == stringToIntegerLabelKeyMap.get(label)) {
            throw new IllegalArgumentException("Label " + label + " does not exist in the " +
                "database.");
        }
        return stringToIntegerLabelKeyMap.get(label);
    }

    public int getNodePropertyKey(String vertexProperty) {
        if (null == stringToIntegerNodePropertyKeyMap.get(vertexProperty)) {
            throw new IllegalArgumentException("Vertex Property " + vertexProperty + " does not " +
                "exist in the database.");
        }
        return stringToIntegerNodePropertyKeyMap.get(vertexProperty);
    }

    public DataType getNodePropertyDataType(int vertexPropertyKey) {
        if (null == nodePropertyKeyToDataTypeMap.get(vertexPropertyKey)) {
            throw new IllegalArgumentException("Vertex Property " + vertexPropertyKey + " does " +
                "not exist in the database.");
        }
        return nodePropertyKeyToDataTypeMap.get(vertexPropertyKey);
    }

    public int getRelPropertyKey(String edgeProperty) {
        if (null == stringToIntegerRelPropertyKeyMap.get(edgeProperty)) {
            throw new IllegalArgumentException("Rel Property " + edgeProperty + " does not " +
                "exist in the database.");
        }
        return stringToIntegerRelPropertyKeyMap.get(edgeProperty);
    }

    public DataType getRelPropertyDataType(int edgePropertyKey) {
        if (null == relPropertyKeyToDataTypeMap.get(edgePropertyKey)) {
            throw new IllegalArgumentException("Property " + edgePropertyKey + " does not exist " +
                "in the database.");
        }
        return relPropertyKeyToDataTypeMap.get(edgePropertyKey);
    }

    public synchronized int insertNodePropertyKeyIfNeeded(String key, DataType dataType) {
        if (stringToIntegerNodePropertyKeyMap.containsKey(key)) {
            return stringToIntegerNodePropertyKeyMap.get(key);
        }
        stringToIntegerNodePropertyKeyMap.put(key, nextNodePropertyKey);
        nodePropertyKeyToDataTypeMap.put(nextNodePropertyKey, dataType);
        return nextNodePropertyKey++;
    }

    public synchronized int insertRelPropertyKeyIfNeeded(String key, DataType dataType) {
        if (stringToIntegerRelPropertyKeyMap.containsKey(key)) {
            return stringToIntegerRelPropertyKeyMap.get(key);
        }
        stringToIntegerRelPropertyKeyMap.put(key, nextRelPropertyKey);
        relPropertyKeyToDataTypeMap.put(nextRelPropertyKey, dataType);
        return nextRelPropertyKey++;
    }

    public void addTypeLabelForDirection(int type, int label, Direction direction) {
        if (Direction.FORWARD == direction) {
            addTypeAndLabelForDirection(typeToDefaultAdjListIndexLabelsFwdMap, labelToTypesFwdMap
                , type, label);
        } else {
            addTypeAndLabelForDirection(typeToDefaultAdjListIndexLabelsBwdMap, labelToTypesBwdMap,
                type, label);
        }
    }

    private void addTypeAndLabelForDirection(List<List<Integer>> typeToLabelMap,
        List<List<Integer>> labelToTypeMap, int type, int label) {
        labelToTypeMap.get(label).add(type);
        typeToLabelMap.get(type).add(label);
    }

    public void setNumPropertiesForLabel(int label, int numProperties) {
        if (labelToNumPropertiesMap.size() > label) {
            labelToNumPropertiesMap.set(label, numProperties);
        }
    }

    public boolean typeLabelExistsForDirection(int type, int label, Direction direction) {
        return getLabelToTypeMapInDirection(direction).get(label).contains(type);
    }

    public boolean labelDirectionHasMultiplicityOne(int label, Direction direction) {
        var cardinality = labelToCardinalityMap.get(label);
        if (Direction.FORWARD == direction) {
            return Cardinality.ONE_TO_ONE == cardinality || Cardinality.MANY_TO_ONE == cardinality;
        } else {
            return Cardinality.ONE_TO_MANY == cardinality || Cardinality.ONE_TO_ONE == cardinality;
        }
    }

    public boolean labelDirectionHasSingleNbrType(int label, Direction direction) {
        return getLabelToNbrTypeMapInDirection(direction).get(label).size() == 1;
    }

    public List<List<Integer>> getLabelToTypeMapInDirection(Direction direction) {
        if (Direction.FORWARD == direction) {
            return labelToTypesFwdMap;
        } else {
            return labelToTypesBwdMap;
        }
    }

    public List<List<Integer>> getLabelToNbrTypeMapInDirection(Direction direction) {
        if (Direction.FORWARD == direction) {
            return labelToTypesBwdMap;
        } else {
            return labelToTypesFwdMap;
        }
    }

    public List<List<Integer>> getTypeToDefaultAdjListIndexLabelsMapInDirection(Direction direction) {
        if (Direction.FORWARD == direction) {
            return typeToDefaultAdjListIndexLabelsFwdMap;
        } else {
            return typeToDefaultAdjListIndexLabelsBwdMap;
        }
    }

    public boolean labelHasProperties(int label) {
        return labelToNumPropertiesMap.get(label) > 0;
    }

    public void print() {
        System.out.println("node types");
        for (var e : stringToIntegerTypeKeyMap.entrySet()) {
            System.out.println(e.getKey() + "->" + e.getValue());
        }
        System.out.println("rel labels");
        for (var e : stringToIntegerLabelKeyMap.entrySet()) {
            System.out.println(e.getKey() + "->" + e.getValue());
        }
        System.out.println("node properties");
        for (var e : stringToIntegerNodePropertyKeyMap.entrySet()) {
            System.out.println(e.getKey() + "->" + e.getValue());
        }
        System.out.println("rel properties");
        for (var e : stringToIntegerRelPropertyKeyMap.entrySet()) {
            System.out.println(e.getKey() + "->" + e.getValue());
        }
    }
}
