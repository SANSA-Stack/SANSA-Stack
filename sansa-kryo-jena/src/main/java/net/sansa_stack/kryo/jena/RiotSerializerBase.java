package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Base class for jena-riot based serializers.
 * Lang is for reading whereas format is a flavor for writing (e.g. format may include pretty formatting).
 * Obviously, the format's language must match to one given for lang.
 *
 * @author Claus Stadler
 */
public abstract class RiotSerializerBase<T> extends Serializer<T> {
    protected Lang lang;
    protected RDFFormat format;

    public RiotSerializerBase(Lang lang, RDFFormat format) {
        this.lang = lang;
        this.format = format;
    }

    protected abstract void writeActual(T obj, OutputStream out);
    protected abstract T readActual(InputStream in);

    @Override
    public final void write(Kryo kryo, Output output, T obj) {
        ByteArrayOutputStream tmp = new ByteArrayOutputStream();
        writeActual(obj, tmp);

        byte[] bytes = tmp.toByteArray();
        ByteArrayUtils.write(output, bytes);
    }

    @Override
    public final T read(Kryo kryo, Input input, Class<T> objClass) {
        byte[] bytes = ByteArrayUtils.read(input);
        ByteArrayInputStream tmp = new ByteArrayInputStream(bytes);
        T result = readActual(tmp);
        return result;
    }
}