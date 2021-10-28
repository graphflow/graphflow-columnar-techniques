package ca.waterloo.dsg.graphflow.storage.loader.fileiterator;

public class LineTokensIterator {

    private byte[][] bufferPairs;
    private int startPointer, endPointer, current;
    private char separator;
    private boolean isTokenAvailable, endOfLine;

    LineTokensIterator(char separator, byte[][] bufferPairs) {
        this.separator = separator;
        this.bufferPairs = bufferPairs;
    }

    public void init(int startPointer, int endPointer) {
        this.startPointer = startPointer;
        this.current = startPointer;
        this.endPointer = endPointer;
        endOfLine = false;
        isTokenAvailable = false;
    }

    public boolean hasMoreTokens() {
        if (endOfLine) {
            return false;
        } else if (isTokenAvailable) {
            return true;
        } else if (current < endPointer) {
            TraceNextToken();
            isTokenAvailable = true;
            return true;
        }
        endOfLine = true;
        return false;
    }

    public boolean isTokenEmpty() {
        return startPointer == current;
    }

    public void skipToken() {
        if (isTokenAvailable) {
            current++;
            startPointer = current;
            isTokenAvailable = false;
            return;
        }
        hasMoreTokens();
        if (isTokenAvailable) {
            skipToken();
            return;
        }
        throw new IllegalStateException("No more tokens left.");
    }

    public byte[] getTokenAsString() {
        if (isTokenAvailable) {
            var val = parseString();
            current++;
            startPointer++;
            isTokenAvailable = false;
            return val;
        }
        hasMoreTokens();
        if (isTokenAvailable) {
            return getTokenAsString();
        }
        throw new IllegalStateException("No more tokens left.");
    }

    public int getTokenAsInteger() {
        if (isTokenAvailable) {
            var val = parseInteger();
            current++;
            startPointer++;
            isTokenAvailable = false;
            return val;
        }
        hasMoreTokens();
        if (isTokenAvailable) {
            return getTokenAsInteger();
        }
        throw new IllegalStateException("No more tokens left.");
    }

    public double getTokenAsDouble() {
        if (isTokenAvailable) {
            var val = parseDouble();
            current++;
            startPointer++;
            isTokenAvailable = false;
            return val;
        }
        hasMoreTokens();
        if (isTokenAvailable) {
            return getTokenAsDouble();
        }
        throw new IllegalStateException("No more tokens left.");
    }

    public boolean getTokenAsBoolean() {
        if (isTokenAvailable) {
            var val = parseBoolean();
            current++;
            startPointer++;
            isTokenAvailable = false;
            return val;
        }
        hasMoreTokens();
        if (isTokenAvailable) {
            return getTokenAsBoolean();
        }
        throw new IllegalStateException("No more tokens left.");
    }

    public long getTokenAsLong() {
        if (isTokenAvailable) {
            var val = parseLong();
            current++;
            startPointer++;
            isTokenAvailable = false;
            return val;
        }
        hasMoreTokens();
        if (isTokenAvailable) {
            return getTokenAsInteger();
        }
        throw new IllegalStateException("No more tokens left.");
    }

    private byte[] parseString() {
        var i = 0;
        var val = new byte[current - startPointer];
        for (; startPointer < current; startPointer++) {
            val[i++] = getByteAt(startPointer);
        }
        return val;
    }

    private int parseInteger() {
        var isNegative = false;
        var state = 0;
        var result  = 0;
        for (; startPointer < current; startPointer++) {
            var nextChar = getByteAt(startPointer);
            if (state == 0) {
                if (nextChar >= 48 && nextChar <= 57) {
                    result = (result * 10) + (nextChar - 48);
                    state = 1;
                } else if (nextChar == '-') {
                    isNegative = true;
                    state = 2;
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                        "state %d, char %c", state, nextChar));
                }
            } else if (state == 1) {
                if (nextChar >= 48 && nextChar <= 57) {
                    result = (result * 10) + (nextChar - 48);
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                        "state %d, char %c", state, nextChar));
                }
            } else if (state == 2) {
                if (nextChar >= 48 && nextChar <= 57) {
                    result = (result * 10) + (nextChar - 48);
                    state = 1;
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                        "state %d, char %c", state, nextChar));
                }
            } else {
                throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                    "state %d, char %c", state, nextChar));
            }
        }
        if (!(state == 1)) {
            throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                "end state %d", state));
        }
        if (isNegative) {
            result = -result;
        }
        return result;
    }

    private double parseDouble() {
        var state = 0;
        var decimalL = 0.0;
        var decimalR = 0.0;
        var factor = 1.0;
        var power10 = 0;
        var isExponentNegative = false;
        var isNegative = false;
        for (; startPointer < current; startPointer++) {
            var nextChar = getByteAt(startPointer);
            if (state == 0) {
                if (nextChar >= 48 && nextChar <= 57) {
                    decimalL = (decimalL * 10.0) + (nextChar - 48);
                    state = 1;
                } else if (nextChar == '-') {
                    isNegative = true;
                    state = 2;
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Double, " +
                        "end state %d", state));
                }
            } else if (state ==  2) {
                if (nextChar >= 48 && nextChar <= 57) {
                    decimalL = (decimalL * 10.0) + (nextChar - 48);
                    state = 1;
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Double, " +
                        "end state %d", state));
                }
            } else if (state == 1) {
                if (nextChar >= 48 && nextChar <= 57) {
                    decimalL = (decimalL * 10.0) + (nextChar - 48);
                } else if (nextChar == '.') {
                    state = 3;
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Double, " +
                        "end state %d", state));
                }
            } else if (state == 3) {
                if (nextChar >= 48 && nextChar <= 57) {
                    factor *= 10;
                    decimalR = (decimalR * 10.0) + (nextChar - 48);
                    state = 4;
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Double, " +
                        "end state %d", state));
                }
            } else if (state == 4) {
                if (nextChar >= 48 && nextChar <= 57) {
                    factor *= 10;
                    decimalR = (decimalR * 10.0) + (nextChar - 48);
                } else if (nextChar == 'e' || nextChar == 'E') {
                    state = 5;
                }
            } else if (state == 5) {
                if (nextChar >= 48 && nextChar <= 57) {
                    power10 = (power10 * 10) + (nextChar - 48);
                    state = 6;
                } else if (nextChar == '-') {
                    isExponentNegative = true;
                    state = 7;
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Double, " +
                        "end state %d", state));
                }
            } else if (state == 6) {
                if (nextChar >= 48 && nextChar <= 57) {
                    power10 = (power10 * 10) + (nextChar - 48);
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Double, " +
                        "end state %d", state));
                }
            } else if (state == 7) {
                if (nextChar >= 48 && nextChar <= 57) {
                    power10 = (power10 * 10) + (nextChar - 48);
                    state = 6;
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Double, " +
                        "end state %d", state));
                }
            } else {
                throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                    "state %d, char %c", state, nextChar));
            }
        }
        if (!(state == 1 || state == 4 || state == 6)) {
            throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                "end state %d", state));
        }
        if (isExponentNegative) {
            power10 = - power10;
        }
        var exp = Math.pow(10, power10);
        decimalR /= factor;
        decimalL += decimalR;
        decimalL *= exp;
        if (isNegative) {
            decimalL = -decimalL;
        }
        return decimalL;
    }

    private long parseLong() {
        var isNegative = false;
        var state = 0;
        long result  = 0L;
        for (; startPointer < current; startPointer++) {
            var nextChar = getByteAt(startPointer);
            if (state == 0) {
                if (nextChar >= 48 && nextChar <= 57) {
                    result = (result * 10) + (nextChar - 48);
                    state = 1;
                } else if (nextChar == '-') {
                    isNegative = true;
                    state = 2;
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                        "state %d, char %c", state, nextChar));
                }
            } else if (state == 1) {
                if (nextChar >= 48 && nextChar <= 57) {
                    result = (result * 10) + (nextChar - 48);
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                        "state %d, char %c", state, nextChar));
                }
            } else if (state == 2) {
                if (nextChar >= 48 && nextChar <= 57) {
                    result = (result * 10) + (nextChar - 48);
                    state = 1;
                } else {
                    throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                        "state %d, char %c", state, nextChar));
                }
            } else {
                throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                    "state %d, char %c", state, nextChar));
            }
        }
        if (!(state == 1)) {
            throw new IllegalStateException(String.format("Cannot parse to Integer, " +
                "end state %d", state));
        }
        if (isNegative) {
            result = -result;
        }
        return result;
    }

    private boolean parseBoolean() {
        var state = 0;
        for (; startPointer < current; startPointer++) {
            var nextChar = getByteAt(startPointer);
            if (state == 0) {
                if (nextChar == 'T' || nextChar == 't') {
                    state = 1;
                } else {
                    startPointer = current;
                    return false;
                }
            } else if (state == 1) {
                if (nextChar == 'R' || nextChar == 'r') {
                    state = 2;
                } else {
                    startPointer = current;
                    return false;
                }
            } else if (state == 2) {
                if (nextChar == 'U' || nextChar == 'u') {
                    state = 3;
                } else {
                    startPointer = current;
                    return false;
                }
            } else if (state == 3) {
                if (nextChar == 'E' || nextChar == 'e') {
                    state = 4;
                } else {
                    startPointer = current;
                    return false;
                }
            } else {
                startPointer = current;
                return false;
            }
        }
        startPointer = current;
        return state == 4;
    }

    private void TraceNextToken() {
        while (current < endPointer && getByteAt(current) != separator) {
            current++;
        }
    }

    private byte getByteAt(int idx) {
        return bufferPairs[idx / bufferPairs[0].length][idx % bufferPairs[0].length];
    }
}
