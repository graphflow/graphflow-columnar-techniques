package ca.waterloo.dsg.graphflow.parser.query.singlequery;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.tuple.Schema;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class MatchGraphSchemaUtils {

    public static Set<RelVariable> getRelVariables(Schema schema, NodeVariable nodeVariable1,
        NodeVariable nodeVariable2) {
        var retVal = new HashSet<RelVariable>();
        for (var relVariable : schema.getRelVariables()) {
            if (relVariable.getSrcNode() == nodeVariable1 &&
                relVariable.getDstNode() == nodeVariable2) {
                retVal.add(relVariable);
            }
            if (relVariable.getSrcNode() == nodeVariable2 &&
                relVariable.getDstNode() == nodeVariable1) {
                retVal.add(relVariable);
            }
        }
        return retVal;
    }

    public static Set<NodeVariable> getNeighborNodeVariables(Schema schema,
        Collection<NodeVariable> nodeVariables) {
        var neighbourNodeVariables = new HashSet<NodeVariable>();
        for (var nodeVariable : nodeVariables) {
            neighbourNodeVariables.addAll(getNeighborNodeVariables(schema, nodeVariable));
        }
        neighbourNodeVariables.removeAll(nodeVariables);
        return neighbourNodeVariables;
    }

    public static Set<NodeVariable> getNeighborNodeVariables(Schema schema,
        NodeVariable nodeVariable) {
        var neighbourNodeVarNames = new HashSet<NodeVariable>();
        for (var relVariable : schema.getRelVariables()) {
            if (relVariable.getSrcNode() == nodeVariable) {
                neighbourNodeVarNames.add(relVariable.getDstNode());
            }
            if (relVariable.getDstNode() == nodeVariable) {
                neighbourNodeVarNames.add(relVariable.getSrcNode());
            }
        }
        return neighbourNodeVarNames;
    }

    public static boolean isConnected(Schema schema) {
        var visited = new HashSet<NodeVariable>();
        var nodeVariables = schema.getNodeVariables();
        var numVertices = nodeVariables.size();
        var src = nodeVariables.iterator().next();
        visited.add(src);
        var frontier = new HashSet<NodeVariable>();
        frontier.add(src);
        while(!frontier.isEmpty()) {
            var nextFrontier = new HashSet<NodeVariable>();
            for (var node : frontier) {
                for (var nbr : getNeighborNodeVariables(schema, node)) {
                    if (!visited.contains(nbr)) {
                        visited.add(nbr);
                        nextFrontier.add(nbr);
                    }
                }
            }
            if (visited.size() == numVertices) {
                return true;
            }
            frontier = nextFrontier;
        }
        return false;
    }

    public static Schema getProjection(Schema schema, Collection<NodeVariable> nodeVariablesToInclude) {
        Schema subGraphMatchSchema = new Schema();
        for (var nodeVariable : schema.getNodeVariables()) {
            if (nodeVariablesToInclude.contains(nodeVariable)) {
                subGraphMatchSchema.addNodeVariable(nodeVariable);
            }
        }
        for (var relVariable : schema.getRelVariables()) {
            if (nodeVariablesToInclude.contains(relVariable.getSrcNode()) &&
                nodeVariablesToInclude.contains(relVariable.getDstNode())) {
                subGraphMatchSchema.addRelVariable(relVariable);
            }
        }
        return subGraphMatchSchema;
    }
}
