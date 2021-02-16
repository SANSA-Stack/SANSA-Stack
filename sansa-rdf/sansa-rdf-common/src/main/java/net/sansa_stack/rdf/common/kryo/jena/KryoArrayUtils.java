package net.sansa_stack.rdf.common.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.lang.reflect.Array;

public class KryoArrayUtils {

    public static <T> void write(Kryo kyro, Output output, T[] items) {
        int len = items.length;
        output.writeInt(len);
        for (T item : items) {
            kyro.writeClassAndObject(output, item);
        }
    }

    public static <T> T[] read(Kryo kyro, Input input, Class<T> clazz) {
        int len = input.readInt(true);
        T[] result = (T[]) Array.newInstance(clazz, len);
        for (int i = 0; i < len; ++i) {
            Object obj = kyro.readClassAndObject(input);
            result[i] = (T)obj;
        }
        return result;
    }
}
