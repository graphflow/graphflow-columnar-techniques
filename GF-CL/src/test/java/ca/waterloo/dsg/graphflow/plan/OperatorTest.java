package ca.waterloo.dsg.graphflow.plan;

import ca.waterloo.dsg.graphflow.parser.query.expressions.ComparisonExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.IntLiteral;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.extend.ExtendAdjLists;
import ca.waterloo.dsg.graphflow.plan.operator.extend.ExtendColumn;
import ca.waterloo.dsg.graphflow.plan.operator.filter.Filter;
import ca.waterloo.dsg.graphflow.plan.operator.flatten.Flatten;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.NodePropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.PropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.scan.Scan;
import ca.waterloo.dsg.graphflow.plan.operator.sink.SinkCountChunks;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.util.DataLoader;
import ca.waterloo.dsg.graphflow.util.datatype.ComparisonOperator;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static ca.waterloo.dsg.graphflow.storage.GraphCatalog.ANY;

public class OperatorTest {

    static Graph graphTinySnb;

    /* expected property values for PERSON node. */
    String[] expectedFName = { "Alice", "Bob", "Carol", "Dan", "Elizabeth", "Farooq", "Greg" };
    boolean[] expectedIsStudent = { true, true, false, false, false, true, false };
    double[] expectedEyeSight = { 5.0, 5.1, 5.0, 4.8, 4.7, 4.5, 4.9 };
    boolean[] expectedIsWorker = { false, false, true, true, true, false, false };
    int[] expectedGender = { 1, 2, 1, 2, 1, 2, 2 };
    int[] expectedAge = { 35, 30, 45, 20, 20, 25, 40 };

    int[] expectedDate = { 1234567890, 1234567890, 1234567890, 1234567890, 1234567892, 1234567892,
        1234567890, 1234567892, 1234567893, 1234567890, 1234567892, 1234567893, 1234567897,
        1234567897
    };

    @BeforeAll
    public static void setUp() {
        graphTinySnb = DataLoader.getDataset("tiny-snb").graph;
    }

    @Test
    public void testScanPerson() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCountChunks(new String[]{},
            new Scan(new NodeVariable("a", catalog.getTypeKey("PERSON")), false));
        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(7, sink.getNumOutTuples());
    }

    @Test
    public void testScanAllNodesPersonAndNodePropertyReaders() {
        var catalog = graphTinySnb.getGraphCatalog();

        var sink = new SinkCountChunks(new String[]{},
            makeNodePropReader("a", "PERSON", DataType.INT, "age", false /*isFlat*/, false /*isFiltered*/,
                makeNodePropReader("a", "PERSON", DataType.INT, "gender", false /*isFlat*/, false /*isFiltered*/,
                    makeNodePropReader("a", "PERSON", DataType.BOOLEAN, "isWorker", false /*isFlat*/, false /*isFiltered*/,
                        makeNodePropReader("a", "PERSON", DataType.DOUBLE, "eyeSight", false /*isFlat*/, false /*isFiltered*/,
                            makeNodePropReader("a", "PERSON", DataType.BOOLEAN, "isStudent", false /*isFlat*/, false /*isFiltered*/,
                                makeNodePropReader("a", "PERSON", DataType.STRING, "fName", false /*isFlat*/, false /*isFiltered*/,
                                    new Scan(new NodeVariable("a", catalog.getTypeKey("PERSON")), false)
                                )
                            )
                        )
                    )
                )
            )
        );

        sink.init(graphTinySnb);
        sink.execute();

        var dataChunks = sink.getDataChunks();
        var vectorFName = dataChunks.getValueVector("a.fName");
        var vectorIsStudent = dataChunks.getValueVector("a.isStudent");
        var vectorEyeSight = dataChunks.getValueVector("a.eyeSight");
        var vectorIsWorker = dataChunks.getValueVector("a.isWorker");
        var vectorGender = dataChunks.getValueVector("a.gender");
        var vectorAge = dataChunks.getValueVector("a.age");

        for (var i = 0; i < expectedFName.length; i++) {
            Assertions.assertEquals(expectedFName[i], vectorFName.getString(i));
            Assertions.assertEquals(expectedIsStudent[i], vectorIsStudent.getBoolean(i));
            Assertions.assertEquals(expectedEyeSight[i], vectorEyeSight.getDouble(i));
            Assertions.assertEquals(expectedIsWorker[i], vectorIsWorker.getBoolean(i));
            Assertions.assertEquals(expectedGender[i], vectorGender.getInt(i));
            Assertions.assertEquals(expectedAge[i], vectorAge.getInt(i));
        }
        Assertions.assertEquals(7, sink.getNumOutTuples());
    }

    @Test
    public void testScanAndFilters() {
        var catalog = graphTinySnb.getGraphCatalog();

        var agePropVar = new PropertyVariable(new NodeVariable("a",
            graphTinySnb.getGraphCatalog().getTypeKey("PERSON")), "age");
        agePropVar.setDataType(DataType.INT);
        var ageGreaterOrEqualTo30 = new ComparisonExpression(ComparisonOperator.
            GREATER_THAN_OR_EQUAL, agePropVar, new IntLiteral(30));

        var sink = new SinkCountChunks(new String[]{},
            new Filter(ageGreaterOrEqualTo30, false /*isFlat*/,
                makeNodePropReader("a", "PERSON", DataType.INT, "age", false /*isFlat*/, false /*isFiltered*/,
                    new Scan(new NodeVariable("a", catalog.getTypeKey("PERSON")), true)
                )
            )
        );

        sink.init(graphTinySnb);
        sink.execute();

        var dataChunks = sink.getDataChunks();
        var valueVector = dataChunks.getValueVector("a.age");
        var ints = valueVector.getInts();
        int[] expectedAge = { 35, 30, 45, 40 };
        for (var i = 0; i < valueVector.state.size; i++) {
            var pos = valueVector.state.selectedValuesPos[i];
            Assertions.assertEquals(expectedAge[i], ints[pos]);
        }
        Assertions.assertEquals(4, sink.getNumOutTuples());
    }

    @Test
    public void testScanOrganisation() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCountChunks(new String[]{},
            new Scan(new NodeVariable("a", catalog.getTypeKey("ORGANISATION")), false)
        );
        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(3, sink.getNumOutTuples());
    }

    @Test
    public void test1hop() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCountChunks(new String[]{},
            new ExtendAdjLists(makeALD(catalog, "a", "PERSON", "b", "PERSON", "e1", "KNOWS", Direction.FORWARD), ANY, false,
                new Flatten("a",
                    new Scan(new NodeVariable("a", catalog.getTypeKey("PERSON")), false)
                )
            )
        );
        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(14, sink.getNumOutTuples());
    }

    @Test
    public void test2hop() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCountChunks(new String[]{},
            new ExtendAdjLists(makeALD(catalog, "b", "PERSON", "c", "PERSON", "e2", "KNOWS", Direction.FORWARD), ANY, false,
                new Flatten("b",
                    new ExtendAdjLists(makeALD(catalog, "a", "PERSON", "b", "PERSON", "e1", "KNOWS", Direction.FORWARD), ANY, false,
                        new Flatten("a",
                            new Scan(new NodeVariable("a", catalog.getTypeKey("PERSON")), false)
                        )
                    )
                )
            )
        );
        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(36, sink.getNumOutTuples());
    }

    @Test
    public void test3hop() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCountChunks(new String[]{},
            new ExtendAdjLists(makeALD(catalog, "c", "PERSON", "d", "PERSON", "e3", "KNOWS", Direction.FORWARD), ANY, false,
                new Flatten("c",
                    new ExtendAdjLists(makeALD(catalog, "b", "PERSON", "c", "PERSON", "e2", "KNOWS", Direction.FORWARD), ANY, false,
                        new Flatten("b",
                            new ExtendAdjLists(makeALD(catalog, "a", "PERSON", "b", "PERSON", "e1", "KNOWS", Direction.FORWARD), ANY, false,
                                new Flatten("a",
                                    new Scan(new NodeVariable("a", catalog.getTypeKey("PERSON")), false)
                                )
                            )
                        )
                    )
                )
            )
        );
        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(108, sink.getNumOutTuples());
    }

    @Test
    public void test3star() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCountChunks(new String[]{},
            new ExtendAdjLists(makeALD(catalog, "a", "PERSON", "d", "PERSON", "e3", "KNOWS", Direction.FORWARD), ANY, false,
                new ExtendAdjLists(makeALD(catalog, "a", "PERSON", "c", "PERSON", "e2", "KNOWS", Direction.FORWARD), ANY, false,
                    new ExtendAdjLists(makeALD(catalog, "a", "PERSON", "b", "PERSON", "e1", "KNOWS", Direction.FORWARD), ANY, false,
                        new Flatten("a",
                            new Scan(new NodeVariable("a", catalog.getTypeKey("PERSON")), false)
                        )
                    )
                )
            )
        );
        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(116, sink.getNumOutTuples());
    }

    @Test
    public void testColExtend() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCountChunks(new String[]{},
            new ExtendColumn(makeALD(catalog, "a", "PERSON", "b", "ORGANISATION", "e1", "STUDYAT", Direction.FORWARD),
                ANY, new Scan(new NodeVariable("a", catalog.getTypeKey("PERSON")), true)
            )
        );
        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(3, sink.getNumOutTuples());
    }

    public PropertyReader makeNodePropReader(String variable, String nodeType, DataType dataType,
        String property, boolean isFlat, boolean isFiltered, Operator prev) {
        var typeKey = graphTinySnb.getGraphCatalog().getTypeKey(nodeType);
        var propVar = new PropertyVariable(new NodeVariable(variable, typeKey), property);
        propVar.setDataType(dataType);
        propVar.setPropertyKey(graphTinySnb.getGraphCatalog().getNodePropertyKey(property));
        return NodePropertyReader.make(propVar, isFlat, isFiltered, graphTinySnb.getNodePropertyStore(), prev);
    }

    private RelVariable makeRelVar(GraphCatalog catalog, String boundVarName, String boundVarLabel,
        String nbrVarName, String nbrVarLabel, String relVarName, String relVarLabel,
        Direction direction) {
        var boundVar = new NodeVariable(boundVarName, catalog.getTypeKey(boundVarLabel));
        var nbrVar = new NodeVariable(nbrVarName, catalog.getTypeKey(nbrVarLabel));
        return direction == Direction.FORWARD ?
            new RelVariable(relVarName, catalog.getLabelKey(relVarLabel), boundVar, nbrVar) :
            new RelVariable(relVarName, catalog.getLabelKey(relVarLabel), nbrVar, boundVar);
    }

    public AdjListDescriptor makeALD(GraphCatalog catalog, String boundVarName,
        String boundVarLabel, String nbrVarName, String nbrVarLabel, String relVarName,
        String relVarLabel, Direction direction) {
        var boundVar = new NodeVariable(boundVarName, catalog.getTypeKey(boundVarLabel));
        var nbrVar = new NodeVariable(nbrVarName, catalog.getTypeKey(nbrVarLabel));
        var relVar = new RelVariable(relVarName, catalog.getLabelKey(relVarLabel), boundVar, nbrVar);
        return new AdjListDescriptor(relVar, boundVar, nbrVar, direction);
    }
}
