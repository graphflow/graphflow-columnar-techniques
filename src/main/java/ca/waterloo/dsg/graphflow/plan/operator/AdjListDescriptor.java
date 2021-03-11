package ca.waterloo.dsg.graphflow.plan.operator;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import lombok.Getter;

import java.io.Serializable;

public class AdjListDescriptor implements Serializable {

    @Getter private RelVariable relVariable;
    @Getter private NodeVariable boundNodeVariable, toNodeVariable;
    @Getter private Direction direction;

    public AdjListDescriptor(RelVariable relVariable, NodeVariable boundNodeVariable,
        NodeVariable toNodeVariableString, Direction direction) {
        this.boundNodeVariable = boundNodeVariable;
        this.toNodeVariable = toNodeVariableString;
        this.relVariable = relVariable;
        this.direction = direction;
    }

    public int getLabel() {
        return relVariable.getLabel();
    }

    public AdjListDescriptor copy() {
        return new AdjListDescriptor(relVariable, boundNodeVariable, toNodeVariable, direction);
    }
}
