package ca.waterloo.dsg.graphflow.parser.query.singlequery;

import ca.waterloo.dsg.graphflow.parser.query.AbstractQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * In memory representation of a single query (see oC_SingleQuery rule in the grammar). In Cypher
 * each query consists of a linear list of query parts separated by WITH clauses, which we
 * represent by {@link QueryPart} class.
 */
public class SingleQuery extends AbstractQuery {

    public List<QueryPart> queryParts = new ArrayList<>();
}
