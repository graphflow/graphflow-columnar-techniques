package ca.waterloo.dsg.graphflow.util.compression;

public class NullSuppressionUtils {

    public final static int CHUNK_LEN = 16;
    public final static int TWO_POWERED_CHUNK_LEN_MINUS_ONE = (1 << CHUNK_LEN) - 1;
    public static byte[][] map;

    static {
        map = new byte[1 << CHUNK_LEN][CHUNK_LEN];
        for (var mask = 0; mask < 1 << CHUNK_LEN; ++mask) {
            byte num1s = 0;
            for (var i = 0; i < CHUNK_LEN; ++i) {
                map[mask][i] = num1s;
                if ((mask & (1 << i)) > 0) {
                    num1s++;
                }
            }
        }
    }

    public static int getChunkIdx(int offset) {
        return offset / CHUNK_LEN;
    }

    public static int getPosInChunk(int offset) {
        return offset % CHUNK_LEN;
    }
}
