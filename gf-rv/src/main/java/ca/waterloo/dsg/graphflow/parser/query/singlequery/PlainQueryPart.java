package ca.waterloo.dsg.graphflow.parser.query.singlequery;

import ca.waterloo.dsg.graphflow.parser.ParserMethodReturnValue;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.PlainReturnOrWith;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.PlainReturnOrWith.PlainReturn;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.PlainReturnOrWith.PlainReturnBody;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.PlainReturnOrWith.PlainWith;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

public class PlainQueryPart implements ParserMethodReturnValue {

    @Getter List<RelVariable> relVariables = new ArrayList<>();
    @Getter @Setter private Expression whereExpression = null;
    // TODO: Avoid assigning to null.
    @Getter private PlainReturnOrWith plainReturnOrWith;

    public boolean hasWhereExpression() {
        return null != whereExpression;
    }

    public PlainReturnBody getPlainReturnBody() {
        return  plainReturnOrWith.getPlainReturnBody();
    }

    public void setPlainReturn(PlainReturnBody plainReturnBody) {
        this.plainReturnOrWith = new PlainReturn(plainReturnBody);
    }

    public void setPlainWith(PlainReturnBody plainReturnBody) {
        this.plainReturnOrWith = new PlainWith(plainReturnBody);
    }
}
