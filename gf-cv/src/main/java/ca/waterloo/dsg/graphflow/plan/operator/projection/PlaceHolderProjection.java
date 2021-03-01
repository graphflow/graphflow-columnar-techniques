package ca.waterloo.dsg.graphflow.plan.operator.projection;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.graph.Graph;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.tuple.Tuple;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PlaceHolderProjection extends Operator {

    public PlaceHolderProjection(Schema inSchema, List<Expression> expressions) {
        var outSchema = inSchema.project(getExpressionVariableNames(expressions));
        this.outputTuple = new Tuple(outSchema);
        operatorName = "PlaceHolderProjection ON " + outSchema.getVariableNamesAsString();
    }

    private Set<String> getExpressionVariableNames(List<Expression> expressions) {
        var retVal = new HashSet<String>();
        expressions.forEach(expression -> retVal.add(expression.getVariableName()));
        return retVal;
    }

    @Override
    public void initFurther(Graph graph) {
        this.inputTuple = prev.getOutputTuple();
        // Warning: This is necessary because in EndToEndTests some operators get initialized
        // multiple times. This is because we do shallow copy of operators during plan
        // enumeration and reuse operators across plans. Therefore, although the schema of the
        // tuple stays the same, the Values and multiplicity of the Tuple need to be reassigned.
        this.outputTuple = new Tuple(outputTuple.getSchema());
        for (var variable : outputTuple.getSchema().getVarNames()) {
            outputTuple.append(inputTuple.get(inputTuple.getIdx(variable)), inputTuple.getSchema()
                .getExpression(variable));
        }
    }

    @Override
    public void processNewTuple() {
        next.processNewTuple();
    }
}
