package net.sansa_stack.query.spark.ontop.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.unibz.inf.ontop.com.google.common.collect.ImmutableBiMap;
import it.unibz.inf.ontop.com.google.common.collect.Maps;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * A kryo {@link Serializer} for guava-libraries {@link ImmutableBiMap}.
 */
public class ShadedImmutableBiMapSerializer extends Serializer<ImmutableBiMap<Object, Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = false;
    private static final boolean IMMUTABLE = true;

    public ShadedImmutableBiMapSerializer() {
        super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableBiMap<Object, Object> immutableMap) {
        kryo.writeObject(output, Maps.newHashMap(immutableMap));
    }

    @Override
    public ImmutableBiMap<Object, Object> read(Kryo kryo, Input input, Class<ImmutableBiMap<Object, Object>> type) {
        Map map = kryo.readObject(input, HashMap.class);
        return ImmutableBiMap.copyOf(map);
    }

    /**
     * Creates a new {@link ShadedImmutableBiMapSerializer} and registers its serializer
     * for the several ImmutableList related classes.
     *
     * @param kryo the {@link Kryo} instance to set the serializer on
     */
    public static void registerSerializers(final Kryo kryo) {

        // ImmutableList (abstract class)
        //  +- RegularImmutableList
        //  |   RegularImmutableList
        //  +- SingletonImmutableList
        //  |   Optimized for List with only 1 element.
        //  +- SubList
        //  |   Representation for part of ImmutableList
        //  +- ReverseImmutableList
        //  |   For iterating in reverse order
        //  +- StringAsImmutableList
        //  |   Used by Lists#charactersOf
        //  +- Values (ImmutableTable values)
        //      Used by return value of #values() when there are multiple cells

        final ShadedImmutableBiMapSerializer serializer = new ShadedImmutableBiMapSerializer();

        kryo.register(ImmutableBiMap.class, serializer);
        kryo.register(ImmutableBiMap.of().getClass(), serializer);

        Object o1 = new Object();
        Object o2 = new Object();

        kryo.register(ImmutableBiMap.of(o1, o1).getClass(), serializer);
        kryo.register(ImmutableBiMap.of(o1, o1, o2, o2).getClass(), serializer);

        Map<ShadedImmutableBiMapSerializer.DummyEnum,Object> enumMap = new EnumMap<>(ShadedImmutableBiMapSerializer.DummyEnum.class);
        enumMap.put(DummyEnum.VALUE1, o1);
        enumMap.put(DummyEnum.VALUE2, o2);

        kryo.register(ImmutableBiMap.copyOf(enumMap).getClass(), serializer);

    }

    private enum DummyEnum {
        VALUE1,
        VALUE2
    }
}