package ca.waterloo.dsg.graphflow.parser.query.indexquery;

import ca.waterloo.dsg.graphflow.parser.ParserMethodReturnValue;
import ca.waterloo.dsg.graphflow.parser.query.AbstractQuery;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.InputSchemaMatchWhere;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import lombok.Getter;
import lombok.Setter;

public abstract class CreateIndexQuery extends AbstractQuery implements ParserMethodReturnValue {

    @Getter
    private InputSchemaMatchWhere inputSchemaMatchWhere = new InputSchemaMatchWhere();
    @Getter @Setter
    private String indexName;

    public Schema getMatchGraphSchema() {
        return inputSchemaMatchWhere.getMatchGraphSchema();
    }
}
