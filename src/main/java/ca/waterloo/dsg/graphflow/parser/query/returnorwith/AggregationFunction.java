package ca.waterloo.dsg.graphflow.parser.query.returnorwith;

import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;

public enum AggregationFunction {
    COUNT_STAR,
    MIN,
    MAX,
    SUM;

    public static AggregationFunction getFromString(String functionName) {
        if (null == functionName) {
            throw new MalformedQueryException("Aggregation function name cannot be null.");
        }
        switch (functionName.toUpperCase()) {
            case "MIN" :
                return MIN;
            case "MAX" :
                return MAX;
            case "SUM" :
                return SUM;
            default :
                throw new MalformedQueryException("Aggregation function name is not " +
                    "recognized. functionName: " + functionName);
        }
    }
}
