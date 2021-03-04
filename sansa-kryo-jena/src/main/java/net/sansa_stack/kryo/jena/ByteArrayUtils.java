package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Utils for writing and reading byte arrays by prefixing the data with its length
 *
 * @author Claus Stadler
 */
public class ByteArrayUtils {
    public static void write(Output output, byte[] bytes) {
        output.writeInt(bytes.length, true);
        output.writeBytes(bytes);
    }

    public static void write(Output output, byte[] bytes, int offset, int length) {
        output.writeInt(length, true);
        output.writeBytes(bytes, offset, length);
    }

    public static byte[] read(Input input) {
        int len = input.readInt(true);
        byte[] result = input.readBytes(len);
        return result;
    }
}
