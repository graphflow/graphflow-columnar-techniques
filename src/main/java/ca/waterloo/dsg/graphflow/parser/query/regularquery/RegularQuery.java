package ca.waterloo.dsg.graphflow.parser.query.regularquery;

import ca.waterloo.dsg.graphflow.parser.ParserMethodReturnValue;
import ca.waterloo.dsg.graphflow.parser.query.AbstractQuery;
import ca.waterloo.dsg.graphflow.parser.query.QueryOperation;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.SingleQuery;

import java.util.ArrayList;
import java.util.List;

public class RegularQuery extends AbstractQuery implements ParserMethodReturnValue {

    public List<SingleQuery> singleQueries = new ArrayList<>();

    public RegularQuery() {
        this.setQueryOperation(QueryOperation.REGULAR_QUERY);
    }
}
