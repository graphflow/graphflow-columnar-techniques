package ca.waterloo.dsg.graphflow.storage.graph.properties;

import java.nio.charset.StandardCharsets;

public class UnstructuredPropertyStore {

    public static String EMPTY_STRING = "";

    protected int encodeInt(byte[] array, int ptr, int val) {
        for (var i = 0; i < 4; i++) {
            array[ptr++] = (byte) (val & 0x000000ff);
            val = val >> 8;
        }
        return ptr;
    }

    protected int encodeDouble(byte[] array, int ptr, double val) {
        var bits = Double.doubleToLongBits(val);
        for (var i = 0; i < 8; i++) {
            array[ptr++] = (byte) (bits & 0x000000ff);
            bits = bits >> 8;
        }
        return ptr;
    }

    protected int encodeBoolean(byte[] array, int ptr, boolean val) {
        array[ptr++] = val ? (byte) 1 : (byte) 0;
        return ptr;
    }

    protected int encodeString(byte[] array, int ptr, byte[] val) {
        ptr = encodeInt(array, ptr, val.length);
        System.arraycopy(val, 0, array, ptr, val.length);
        return ptr + val.length;
    }

    protected int searchKey(byte[] array, int key) {
        var i = 0;
        while (i < array.length) {
            if (array[i] == key) {
                return i + 2;
            } else {
                switch (array[i + 1]) {
                    case 2 /*BOOL*/:
                        i += 3;
                        break;
                    case 1 /*DBL*/:
                        i += 8;
                        break;
                    case 0 /*INT*/:
                        i += 6;
                        break;
                    case 3 /*STR*/:
                        var l = decodeInt(array, i + 2);
                        i += (6 + l);
                        break;
                }
            }
        }
        return -1;
    }

    protected int decodeInt(byte[] array, int ptr) {
        var v = 0;
        for (var i = ptr + 3; i >= ptr; i--) {
            v = (v << 8) + (array[i] & 0x000000ff);
        }
        return v;
    }

    protected boolean decodeBoolean(byte[] array, int ptr) {
        return array[ptr] == 1;
    }

    protected double decodeDouble(byte[] array, int ptr) {
        long v = 0L;
        for (var i = ptr + 7; i >= ptr; i--) {
            v = (v << 8) + (array[i] & 0x000000ff);
        }
        return Double.longBitsToDouble(v);
    }

    protected String decodeString(byte[] array, int ptr) {
        var l = decodeInt(array, ptr);
        return new String(array, ptr + 4, l, StandardCharsets.US_ASCII);
    }
}