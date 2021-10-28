package ca.waterloo.dsg.graphflow.query;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.MatchGraphSchemaUtils;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getNodeVariable;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getRelVariable;

/**
 * Tests {@link MatchGraphSchemaUtils}.
 */
public class MatchGraphSchemaUtilsTest {

    private Schema schema;

    @BeforeEach
    public void setUp() {
        schema = new Schema();
        // Create the {@code MatchGraph}. (a)->(b), (b)->(a), (c)->(b), (c)->(a)
        var nodeVariableA = getNodeVariable("a");
        var nodeVariableB = getNodeVariable("b");
        var nodeVariableC = getNodeVariable("c");
        schema.addRelVariable(getRelVariable("e1", nodeVariableA, nodeVariableB));
        schema.addRelVariable(getRelVariable("e2", nodeVariableB, nodeVariableA));
        schema.addRelVariable(getRelVariable("e3", nodeVariableB, nodeVariableC));
        schema.addRelVariable(getRelVariable("e4", nodeVariableC, nodeVariableB));
        schema.addRelVariable(getRelVariable("e5", nodeVariableC, nodeVariableA));
    }

    @Test
    public void testGetAllVariables() {
        schema.addRelVariable(getRelVariable("e6", getNodeVariable("d"), getNodeVariable("e")));
        String[] expectedQueryVariables = {"a", "b", "c", "d", "e"};
        Assertions.assertArrayEquals(expectedQueryVariables, schema.getNodeVariables().stream().
            map(Expression::getVariableName).sorted().toArray());
    }

    @Test
    public void testGetAllNeighbourNodeVariables() {
        var nodeVariables = getNodeVariables(new String[] {"a"});
        var neighbourNodeVariables = MatchGraphSchemaUtils.getNeighborNodeVariables(schema,
            nodeVariables);
        Assertions.assertEquals(getNodeVariables(new String[]{"b", "c"}), neighbourNodeVariables);

        nodeVariables = getNodeVariables(new String[] {"b"});
        neighbourNodeVariables = MatchGraphSchemaUtils.getNeighborNodeVariables(schema,
            nodeVariables);
        Assertions.assertEquals(getNodeVariables(new String[]{"a", "c"}), neighbourNodeVariables);

        nodeVariables = getNodeVariables(new String[] {"a", "c"});
        neighbourNodeVariables = MatchGraphSchemaUtils.getNeighborNodeVariables(schema,
            nodeVariables);
        Assertions.assertEquals(getNodeVariables(new String[]{"b"}), neighbourNodeVariables);
    }

    private Set<NodeVariable> getNodeVariables(String[] nodeVarNames) {
        var nodeVariables = new HashSet<NodeVariable>();
        for (var nodeVarName : nodeVarNames) {
            nodeVariables.add((NodeVariable) schema.getExpression(nodeVarName));
        }
        return nodeVariables;
    }
}
