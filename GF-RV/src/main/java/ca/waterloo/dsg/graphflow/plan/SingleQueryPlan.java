package ca.waterloo.dsg.graphflow.plan;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Class representing a query plan for a Cypher single-query, i.e., a single or multi-part query
 * that currently consists of a reading clauses, such as MATCH, WHERE, WITH, RETURN.
 */
public class SingleQueryPlan implements Serializable {

    @Getter private Operator lastOperator;

    @Getter @Setter double estimatedICost;
    @Getter @Setter double estimatedNumOutTuples;

    public SingleQueryPlan() {}

    /**
     * Constructs a {@link SingleQueryPlan} object.
     *
     * @param lastOperator is the operator to execute.
     */
    public SingleQueryPlan(Operator lastOperator) {
        this.lastOperator = lastOperator;
    }

    /**
     * Appends a new operator to the last operator in the plan.
     *
     * @param newOperator is the operator to append.
     */
    public void append(Operator newOperator) {
        if (null == lastOperator) {
            lastOperator = newOperator;
        } else {
            lastOperator.setNext(newOperator);
            newOperator.setPrev(lastOperator);
            lastOperator = newOperator;
        }
    }

    /**
     * Initialize the plan by initializing all of its operators.
     *
     * @param graph is the input data graph.
     */
    public void init(Graph graph) {
        setNextPointers();
        lastOperator.init(graph);
    }

    public void setNextPointers() {
        if (null != lastOperator) {
            var operator = lastOperator;
            while (null != operator.getPrev()) {
                operator.getPrev().setNext(operator);
                operator = operator.getPrev();
            }
        }
    }

    public SingleQueryPlan getShallowCopy() {
        return new SingleQueryPlan(lastOperator);
    }
}
