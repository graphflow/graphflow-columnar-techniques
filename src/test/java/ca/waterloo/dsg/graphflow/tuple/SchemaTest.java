package ca.waterloo.dsg.graphflow.tuple;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getNodeVariable;
import static ca.waterloo.dsg.graphflow.util.GraphflowTestUtils.getRelVariable;

public class SchemaTest {

    private Schema schema = new Schema();

    @BeforeEach
    public void setup() {
        // (vx)-[ex]->(vy)-[ey]->(vz)-[ez]->(va)
        var nodeVariable1 = getNodeVariable("vx");
        schema.add(nodeVariable1.getVariableName(), nodeVariable1);
        var nodeVariable2 = getNodeVariable("vy");
        schema.add(nodeVariable2.getVariableName(), nodeVariable2);
        var nodeVariable3 = getNodeVariable("vz");
        schema.add(nodeVariable3.getVariableName(), nodeVariable3);
        var nodeVariable4 = getNodeVariable("va");
        schema.add(nodeVariable4.getVariableName(), nodeVariable4);
        var relVariable1 = getRelVariable("ex", nodeVariable1, nodeVariable2);
        schema.add(relVariable1.getVariableName(), relVariable1);
        var relVariable2 = getRelVariable("ey", nodeVariable1, nodeVariable2);
        schema.add(relVariable2.getVariableName(), relVariable2);
        var relVariable3 = getRelVariable("ez", nodeVariable1, nodeVariable2);
        schema.add(relVariable3.getVariableName(), relVariable3);
    }

    @Test
    public void testGetVariablesInLexOrderMethods() {
        var expectedNodeVariables = new HashSet<NodeVariable>();
        expectedNodeVariables.add((NodeVariable) schema.getExpression("vx"));
        expectedNodeVariables.add((NodeVariable) schema.getExpression("vy"));
        expectedNodeVariables.add((NodeVariable) schema.getExpression("vz"));
        expectedNodeVariables.add((NodeVariable) schema.getExpression("va"));
        Assertions.assertEquals(expectedNodeVariables, schema.getNodeVariables());
        var expectedRelVariables= new HashSet<RelVariable>();
        expectedRelVariables.add((RelVariable) schema.getExpression("ex"));
        expectedRelVariables.add((RelVariable) schema.getExpression("ey"));
        expectedRelVariables.add((RelVariable) schema.getExpression("ez"));
        Assertions.assertEquals(expectedRelVariables, schema.getRelVariables());
    }
}
