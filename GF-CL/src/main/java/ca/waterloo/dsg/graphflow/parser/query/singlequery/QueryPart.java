package ca.waterloo.dsg.graphflow.parser.query.singlequery;

import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith;
import ca.waterloo.dsg.graphflow.parser.query.returnorwith.ReturnOrWith.ReturnBody;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Represents a Cypher query part, which consists of set of graph patterns (e.g., found in a MATCH
 * clause) and a set of whereClause (which are specified in WHERE clauses). Contains data
 * structures for fast lookup of query vertices and query edges. A query graph must have at
 * least one edge and must be connected. This class is effectively our relational algebra
 * representation of a Cypher query part.
 */
public class QueryPart implements Serializable {

    @Getter private InputSchemaMatchWhere inputSchemaMatchWhere;
    @Getter @Setter private ReturnOrWith returnOrWith;
    @Getter @Setter private boolean emptyOutputDueToTypeOrLabelMissing = false;

    public QueryPart(Schema inputSchema) {
        this.inputSchemaMatchWhere = new InputSchemaMatchWhere();
        this.inputSchemaMatchWhere.setInputSchema(inputSchema);
    }

    public Schema getMatchGraphSchema() {
        return inputSchemaMatchWhere.getMatchGraphSchema();
    }

    public Schema getOutputSchema() {
        return returnOrWith.getReturnBody().getOutputSchema();
    }

    public ReturnBody getReturnBody() {
        return returnOrWith.getReturnBody();
    }

    @Override
    public String toString() {
        return inputSchemaMatchWhere.toString();
    }

    public Schema getInputSchema() {
        return inputSchemaMatchWhere.getInputSchema();
    }
}
