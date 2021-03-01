package ca.waterloo.dsg.graphflow.util.datatype;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * Represents the possible data types of property values.
 */
public enum DataType implements Serializable {

    BOOLEAN,
    DOUBLE,
    INT,
    STRING,
    NODE,
    RELATIONSHIP,
    UNKNOWN;

    public static final byte TRUE = 1;
    public static final byte FALSE = 2;
    public static final byte NULL_BOOLEAN = (byte) 0;
    public static final int NULL_INTEGER = Integer.MIN_VALUE;
    public static final double NULL_DOUBLE = Double.NEGATIVE_INFINITY;
    public static final double DELTA = Math.pow(10, -324);

    /**
     * Converts the {@code String} resultDataType to the actual {@link DataType} enum.
     *
     * @param dataType The {@code String} resultDataType.
     * @return The {@link DataType} enum obtained from the {@code String} resultDataType.
     * @throws IllegalArgumentException if {@code resultDataType} is not one of the {@link DataType}
     * enum values.
     */
    public static DataType getDataType(String dataType) {
        dataType = dataType.toUpperCase();
        if (INT.toString().equals(dataType)) {
            return INT;
        } else if (DOUBLE.toString().equals(dataType)) {
            return DOUBLE;
        } else if (BOOLEAN.toString().equals(dataType)) {
            return BOOLEAN;
        } else if (STRING.toString().equals(dataType)) {
            return STRING;
        }
        // Should never happen.
        throw new IllegalArgumentException();
    }

    public boolean isNumeric() {
        return this == INT || this == DOUBLE;
    }
    /**
     * Encodes a boolean value as a byte.
     *
     * @param bool is the boolean value to encode.
     * @return the boolean value encoded as a byte.
     */
    public static byte getBooleanValueAsByte(Boolean bool) {
        if (null == bool) {
            return NULL_BOOLEAN;
        }
        if (bool) {
            return TRUE;
        }
        return FALSE;
    }

    /**
     * @param dataType One of the {@link DataType} enum values.
     * @return The number of bytes required to keyStore the {@code resultDataType}.
     * @throws IllegalArgumentException if {@code resultDataType} passed is not one of the {@link
     * DataType} enum values.
     */
    public static int getLength(DataType dataType, String value) {
        if (dataType == STRING) {
            return value.getBytes(StandardCharsets.UTF_8).length;
        } else {
            return getLength(dataType);
        }
    }

    public static int getLength(DataType dataType) {
        switch (dataType) {
            case BOOLEAN:
                return Byte.BYTES;
            case INT:
                return Integer.BYTES;
            case DOUBLE:
                return Double.BYTES;
            default:
                // Should never happen.
                throw new IllegalArgumentException();
        }
    }
}
