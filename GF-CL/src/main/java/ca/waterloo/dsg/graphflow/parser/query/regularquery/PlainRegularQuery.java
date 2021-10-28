package ca.waterloo.dsg.graphflow.parser.query.regularquery;

import ca.waterloo.dsg.graphflow.parser.ParserMethodReturnValue;
import ca.waterloo.dsg.graphflow.parser.query.AbstractQuery;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.PlainSingleQuery;

import java.util.ArrayList;
import java.util.List;

public class PlainRegularQuery extends AbstractQuery implements ParserMethodReturnValue {

    public List<PlainSingleQuery> plainSingleQueries = new ArrayList<>();
}
