package net.sansa_stack.query.spark.ontop.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.unibz.inf.ontop.com.google.common.collect.BiMap;
import it.unibz.inf.ontop.com.google.common.collect.HashBiMap;
import it.unibz.inf.ontop.com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

/**
 * A kryo {@link Serializer} for guava-libraries {@link ImmutableList}.
 */
public class ShadedBiMapSerializer extends Serializer<BiMap<Object, Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = false;
    private static final boolean IMMUTABLE = true;

    public ShadedBiMapSerializer() {
        super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
    }

    @Override
    public void write(Kryo kryo, Output output, BiMap<Object, Object> immutableMap) {
        kryo.writeObject(output, Maps.newHashMap(immutableMap));
    }

    @Override
    public BiMap<Object, Object> read(Kryo kryo, Input input, Class<BiMap<Object, Object>> type) {
        Map map = kryo.readObject(input, HashMap.class);
        return HashBiMap.create(map);
    }

    /**
     * Creates a new {@link ShadedBiMapSerializer} and registers its serializer
     * for the several ImmutableList related classes.
     *
     * @param kryo the {@link Kryo} instance to set the serializer on
     */
    public static void registerSerializers(final Kryo kryo) {
        final ShadedBiMapSerializer serializer = new ShadedBiMapSerializer();

        kryo.register(BiMap.class, serializer);
        kryo.register(HashBiMap.class, serializer);
        kryo.register(HashBiMap.create().getClass(), serializer);


    }
}