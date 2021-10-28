package ca.waterloo.dsg.graphflow.planner;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.QueryParser;
import ca.waterloo.dsg.graphflow.plan.IllegalOperationException;
import ca.waterloo.dsg.graphflow.tuple.Tuple;
import ca.waterloo.dsg.graphflow.tuple.value.Value;
import ca.waterloo.dsg.graphflow.tuple.value.ValueFactory;
import ca.waterloo.dsg.graphflow.tuple.value.flat.DoubleVal;
import ca.waterloo.dsg.graphflow.tuple.value.flat.IntVal;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class UnionTest extends AbstractEndToEndTests {

    @Test
    public void testUnionAllDuplicatesAcrossQueriesAreKept() {
        var queryStr = "MATCH (a:PERSON)-[e0:STUDYAT]->(b:ORGANISATION) " +
            "RETURN e0.year AS eYearnCode, b.mark as markEyeSight UNION ALL " +
            "MATCH (c:PERSON)-[e1:WORKAT]->(d:ORGANISATION) " +
            "RETURN e1.year As eYearnCode, c.eyeSight as markEyeSight";
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("markEyeSight", DataType.DOUBLE));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("eYearnCode", DataType.INT));
        var tupleValues = new ArrayList<Value[]>();
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 3.7),
            new IntVal("eYearnCode", 2021)});
        for (int i = 0; i < 2; ++i) {
            tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 3.7),
                new IntVal("eYearnCode", 2020)});
        }
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 5),
            new IntVal("eYearnCode", 2015)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 4.8),
            new IntVal("eYearnCode", 2010)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 4.7),
            new IntVal("eYearnCode", 2015)});
        assertTest(queryStr, sampleTupleForTable, tupleValues);
    }

    @Test
    public void testUnionDuplicatesAcrossQueriesAreNotKept() {
        var queryStr = "MATCH (a:PERSON)-[e0:STUDYAT]->(b:ORGANISATION) " +
            "RETURN e0.year AS eYearnCode, b.mark as markEyeSight UNION " +
            "MATCH (c:PERSON)-[e1:WORKAT]->(d:ORGANISATION) " +
            "RETURN e1.year As eYearnCode, c.eyeSight as markEyeSight UNION " +
            "MATCH (e:PERSON)-[e2:WORKAT]->(f:ORGANISATION) " +
            "RETURN  f.orgCode As eYearnCode, f.mark as markEyeSight";
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("markEyeSight", DataType.DOUBLE));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("eYearnCode", DataType.INT));
        var tupleValues = new ArrayList<Value[]>();
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 3.7),
            new IntVal("eYearnCode", 2021)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 3.7),
            new IntVal("eYearnCode", 2020)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 5),
            new IntVal("eYearnCode", 2015)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 4.8),
            new IntVal("eYearnCode", 2010)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 4.7),
            new IntVal("eYearnCode", 2015)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 4.1),
            new IntVal("eYearnCode", 934)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 4.1),
            new IntVal("eYearnCode", 824)});
        assertTest(queryStr, sampleTupleForTable, tupleValues);
    }

    @Test
    public void testUnionWithDifferentAlias() {
        Assertions.assertThrows(MalformedQueryException.class, () ->
            QueryParser.parseQuery("MATCH (a:PERSON)-[e:STUDYAT]->(b:ORGANISATION) " +
                "RETURN e.year AS eYear UNION " +
                "MATCH (c:PERSON)-[e1:WORKAT]->(d:ORGANISATION) " +
                "RETURN e1.year As eY", graphTinySnb.getGraphCatalog())
        );
    }

    @Test
    public void testUnionWithDifferentType() {
        Assertions.assertThrows(MalformedQueryException.class, () ->
            QueryParser.parseQuery("MATCH (a:PERSON)-[e:STUDYAT]->(b:ORGANISATION) " +
                "RETURN e.year AS eYearnCode UNION " +
                "MATCH (c:PERSON)-[e1:WORKAT]->(d:ORGANISATION) " +
                "RETURN d.mark As eYearnCode", graphTinySnb.getGraphCatalog())
        );
    }

    @Test
    public void testUnionWithBothUnionAndUnionAll() {
        Assertions.assertThrows(MalformedQueryException.class, () ->
            QueryParser.parseQuery("MATCH (a:PERSON)-[e0:STUDYAT]->(b:ORGANISATION) " +
                "RETURN e0.year AS eYearnCode, b.mark as markEyeSight UNION " +
                "MATCH (c:PERSON)-[e1:WORKAT]->(d:ORGANISATION) " +
                "RETURN e1.year As eYearnCode, c.eyeSight as markEyeSight UNION ALL " +
                "MATCH (e:PERSON)-[e2:]->(f:ORGANISATION) " +
                "RETURN  f.orgCode As eYearnCode, f.mark as markEyeSight",
                graphTinySnb.getGraphCatalog())
        );
    }

    @Test
    public void testUnionStoreTuples() {
        Assertions.assertThrows(IllegalOperationException.class, () -> {
            var queryStr = "MATCH (a:PERSON)-[e0:STUDYAT]->(b:ORGANISATION) " +
                "RETURN e0.year AS eYearnCode, b.mark as markEyeSight UNION " +
                "MATCH (c:PERSON)-[e1:WORKAT]->(d:ORGANISATION) " +
                "RETURN c.eyeSight as markEyeSight, e1.year As eYearnCode";
            var plans = getAllUnfactorizedPlans(queryStr);
            plans.forEach(plan -> plan.setStoreTuples(false));
        });
    }

    @Test
    public void testUnionDuplicatesWithinEachQueryIsNotKept() {
        var queryStr = "MATCH (a:PERSON)-[e0:STUDYAT]->(b:ORGANISATION) " +
            "RETURN e0.year AS eYearnCode, b.mark as markEyeSight UNION " +
            "MATCH (c:PERSON)-[e1:WORKAT]->(d:ORGANISATION) " +
            "RETURN c.eyeSight as markEyeSight, e1.year As eYearnCode";
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("markEyeSight", DataType.
            DOUBLE));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("eYearnCode", DataType.
            INT));
        var tupleValues = new ArrayList<Value[]>();
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 3.7),
            new IntVal("eYearnCode", 2021)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 3.7),
            new IntVal("eYearnCode", 2020)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 5),
            new IntVal("eYearnCode", 2015)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 4.8),
            new IntVal("eYearnCode", 2010)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 4.7),
            new IntVal("eYearnCode", 2015)});
        assertTest(queryStr, sampleTupleForTable, tupleValues);
    }

    @Test
    public void testUnionAllDuplicatesWithinEachQueryIsKept() {
        var queryStr = "MATCH (a:PERSON)-[e0:STUDYAT]->(b:ORGANISATION) " +
            "RETURN e0.year AS eYearnCode, b.mark as markEyeSight UNION ALL " +
            "MATCH (c:PERSON)-[e1:WORKAT]->(d:ORGANISATION) " +
            "RETURN c.eyeSight as markEyeSight, e1.year As eYearnCode";
        var sampleTupleForTable = new Tuple();
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("markEyeSight", DataType.
            DOUBLE));
        sampleTupleForTable.append(ValueFactory.getFlatValueForDataType("eYearnCode", DataType.
            INT));
        var tupleValues = new ArrayList<Value[]>();
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 3.7),
            new IntVal("eYearnCode", 2021)});
        for (int i = 0; i < 2; ++i) {
            tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 3.7),
                new IntVal("eYearnCode", 2020)});
        }
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 5),
            new IntVal("eYearnCode", 2015)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 4.8),
            new IntVal("eYearnCode", 2010)});
        tupleValues.add(new Value[]{new DoubleVal("markEyeSight", 4.7),
            new IntVal("eYearnCode", 2015)});
        assertTest(queryStr, sampleTupleForTable, tupleValues);
    }
}


