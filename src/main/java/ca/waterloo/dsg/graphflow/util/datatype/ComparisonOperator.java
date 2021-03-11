package ca.waterloo.dsg.graphflow.util.datatype;

import lombok.Getter;

/**
 * Comparison enum.
 */
public enum ComparisonOperator {
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL("<="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL(">="),
    EQUALS("="),
    NOT_EQUALS("<>"),
    STARTS_WITH("STARTS WITH"),
    ENDS_WITH("ENDS WITH"),
    CONTAINS("CONTAINS"),
    AND("AND"),
    OR("OR");

    @Getter
    private String symbol;
    ComparisonOperator(String symbol) {
        this.symbol = symbol;
    }

    /**
     * @return Returns the equivalent operator to be used if operands are reversed.
     */
    public ComparisonOperator getReverseOperator() {
        switch (this) {
            case LESS_THAN:
                return GREATER_THAN;
            case GREATER_THAN:
                return LESS_THAN;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL;
            case EQUALS:
                return EQUALS;
            case NOT_EQUALS:
                return NOT_EQUALS;
            case AND:
                return OR;
            case OR:
                return AND;
            default:
                // returns null for comparators whose constructReverse is not defined.
                return null;
        }
    }

    public static ComparisonOperator fromString(String comparisonOperator) {
        switch (comparisonOperator) {
            case "=":
                return EQUALS;
            case "<>":
                return NOT_EQUALS;
            case "<":
                return LESS_THAN;
            case ">":
                return GREATER_THAN;
            case "<=":
                return LESS_THAN_OR_EQUAL;
            case ">=":
                return GREATER_THAN_OR_EQUAL;
            case "STARTS WITH":
                return STARTS_WITH;
            case "ENDS WITH":
                return ENDS_WITH;
            case "CONTAINS":
                return CONTAINS;
            case "AND":
                return AND;
            case "OR":
                return OR;
            default:
                throw new IllegalArgumentException("The comparisonOperator operator (" +
                    comparisonOperator + ") is not supported.");
        }
    }

    @Override
    public String toString() {
        switch (this) {
            case NOT_EQUALS:
                return "<>";
            case LESS_THAN:
                return "<";
            case GREATER_THAN:
                return ">";
            case LESS_THAN_OR_EQUAL:
                return "<=";
            case GREATER_THAN_OR_EQUAL:
                return ">=";
            case STARTS_WITH:
                return "STARTS WITH";
            case ENDS_WITH:
                return "ENDS WITH";
            case CONTAINS:
                return "CONTAINS";
            case AND:
                return "AND";
            case OR:
                return "OR";
            default:
                return "=";
        }
    }
}
