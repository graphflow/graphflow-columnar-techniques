package ca.waterloo.dsg.graphflow.parser;

import ca.waterloo.dsg.graphflow.grammar.GraphflowBaseVisitor;

/**
 * Note: This is a {@link RuntimeException} because the {@link ParseTreeVisitor} needs to
 * throw this exception but because {@link ParseTreeVisitor} extends a generated
 * {@link GraphflowBaseVisitor} class's methods which do not have a throws clause.
 */
public class MalformedQueryException extends RuntimeException {

    public MalformedQueryException(Exception e) {
        super(e);
    }

    public MalformedQueryException(String eMessage) {
        super(eMessage);
    }
}
