package ca.waterloo.dsg.graphflow.parser.query.singlequery;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.ExpressionUtils;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.StringJoiner;

public class InputSchemaMatchWhere {
    @Getter @Setter private Schema inputSchema = new Schema();
    @Getter Schema matchGraphSchema = new Schema();
    @Getter @Setter
    private Expression whereExpression = null;

    public boolean hasWhereExpression() {
        return null != whereExpression;
    }

    public boolean hasConjunctiveWhereExpression() {
        if (null == whereExpression) { return false; }
        return ExpressionUtils.isConjunctivePredicate(whereExpression);
    }

    /**
     * Projects this MatchWhereClause to a set of query vertices.
     *
     * @param nodeVariables The vertices to include in the projection.
     * @return The projected MatchWhereClause.
     */
    public InputSchemaMatchWhere getProjection(Collection<NodeVariable> nodeVariables) {
        var projection = new InputSchemaMatchWhere();
        projection.matchGraphSchema = MatchGraphSchemaUtils.getProjection(matchGraphSchema, nodeVariables);
        if (!hasWhereExpression()) {
            return projection;
        }
        var schemaInScope = Schema.union(inputSchema, projection.getMatchGraphSchema());
        var projectedWhereExpression =
            ExpressionUtils.projectOntoVars(whereExpression, schemaInScope);
        if (null != projectedWhereExpression) {
            projection.setWhereExpression(projectedWhereExpression);
        }
        return projection;
    }

    @Override
    public String toString() {
        var stringJoiner = new StringJoiner("");
        stringJoiner.add(matchGraphSchema.toString());
        if (hasWhereExpression()) {
            stringJoiner.add(whereExpression.getPrintableExpression());
        }
        return stringJoiner.toString();
    }
}
