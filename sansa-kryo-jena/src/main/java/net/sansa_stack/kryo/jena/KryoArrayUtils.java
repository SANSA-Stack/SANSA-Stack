package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.lang.reflect.Array;

/** Utils to write arrays to kryo - probably not needed; the code that uses it is probably legacy ~ Claus Stadler */
public class KryoArrayUtils {

    public static <T> void write(Kryo kryo, Output output, T[] items) {
        int len = items.length;
        output.writeInt(len, true);
        for (T item : items) {
            kryo.writeClassAndObject(output, item);
        }
    }

    public static <T> T[] read(Kryo kryo, Input input, Class<T> clazz) {
        int len = input.readInt(true);
        @SuppressWarnings("unchecked")
        T[] result = (T[]) Array.newInstance(clazz, len);
        for (int i = 0; i < len; ++i) {
            Object obj = kryo.readClassAndObject(input);
            @SuppressWarnings("unchecked")
            T t = (T)obj;
            result[i] = t;
        }
        return result;
    }
}
