package ca.waterloo.dsg.graphflow.parser;

import ca.waterloo.dsg.graphflow.grammar.GraphflowLexer;
import ca.waterloo.dsg.graphflow.grammar.GraphflowParser;
import ca.waterloo.dsg.graphflow.parser.query.AbstractQuery;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.PlainRegularQuery;
import ca.waterloo.dsg.graphflow.parser.query.regularquery.RegularQuery;
import ca.waterloo.dsg.graphflow.parser.query.singlequery.SingleQuery;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;


/**
 * Converts a raw query string into a {@code ParsedQuery} object.
 */
public class QueryParser {

    /**
     * Parses the user query to obtain an implementation of {@link AbstractQuery}, such as a
     * {@link SingleQuery}.
     *
     * @param strQuery is the user query.
     * @return The parsed {@link AbstractQuery}.
     */
    public static AbstractQuery parseQuery(String strQuery, GraphCatalog catalog)
        throws ParseCancellationException {
        try {
            AbstractQuery query = parseAntlr(strQuery + ";", catalog);
            if (query instanceof PlainRegularQuery) {
                var regularQuery = new RegularQuery();
                Schema curSchema = null;
                for (int i = 0; i < ((PlainRegularQuery) query).plainSingleQueries.size(); ++i) {
                    regularQuery.singleQueries.add(
                        ValidatorAndSingleQueryConverter.validateAndRewrite(
                            ((PlainRegularQuery) query).plainSingleQueries.get(i), catalog));
                    var lastQueryParts = regularQuery.singleQueries.get(i).queryParts;
                    if (null == curSchema) {
                        curSchema = lastQueryParts.get(lastQueryParts.size() - 1)
                            .getOutputSchema();
                        continue;
                    }
                    if (!curSchema.isSame(
                        lastQueryParts.get(lastQueryParts.size() - 1).getOutputSchema())) {
                        throw new MalformedQueryException("Each query " +
                            "that is part of a union operation should have the same schema.");
                    }
                }
                return regularQuery;
            }
            return query;
        } catch (Exception e) {
            throw new MalformedQueryException(e);
        }
    }

    private static AbstractQuery parseAntlr(String query, GraphCatalog catalog)
        throws ParseCancellationException {
        GraphflowParser parser = getParser(query);
        var visitor = new ParseTreeVisitor(catalog);
        return (AbstractQuery) visitor.visit(parser.oC_Cypher() /* parseTree */);
    }

    private static GraphflowParser getParser(String query) {
        var lexer = new GraphflowLexer(CharStreams.fromString(query));
        lexer.removeErrorListeners();   // Remove default listeners first.
        lexer.addErrorListener(AntlrErrorListener.INSTANCE);

        var parser = new GraphflowParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();   // Remove default listeners first.
        parser.addErrorListener(AntlrErrorListener.INSTANCE);
        return parser;
    }
}
