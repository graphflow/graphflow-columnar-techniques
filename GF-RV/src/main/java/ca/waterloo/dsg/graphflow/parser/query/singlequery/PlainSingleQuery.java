package ca.waterloo.dsg.graphflow.parser.query.singlequery;

import ca.waterloo.dsg.graphflow.parser.ParserMethodReturnValue;
import ca.waterloo.dsg.graphflow.parser.query.AbstractQuery;

import java.util.ArrayList;
import java.util.List;

public class PlainSingleQuery extends AbstractQuery implements ParserMethodReturnValue {

    public List<PlainQueryPart> plainQueryParts = new ArrayList<>();
}
