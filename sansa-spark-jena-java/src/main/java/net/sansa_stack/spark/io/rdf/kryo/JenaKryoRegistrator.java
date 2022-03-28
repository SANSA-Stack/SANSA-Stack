package net.sansa_stack.spark.io.rdf.kryo;

import com.esotericsoftware.kryo.Kryo;

import com.esotericsoftware.kryo.serializers.BeanSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.aksw.commons.collector.core.SetOverMap;
import org.aksw.jenax.io.kryo.jena.JenaKryoRegistratorLib;
import org.aksw.jenax.io.kryo.jenax.JenaxKryoRegistratorLib;
import org.apache.spark.serializer.KryoRegistrator;

public class JenaKryoRegistrator
    implements KryoRegistrator
{
    @Override
    public void registerClasses(Kryo kryo) {
        JenaKryoRegistratorLib.registerClasses(kryo);
        JenaxKryoRegistratorLib.registerClasses(kryo);

        // SetOverMap is used in NodeAnalytics for used prefix analytics
        // TODO Move to its own kryo registrator
        //  And then have a single kryo serializer for sansa
        kryo.register(SetOverMap.class, new FieldSerializer(kryo, SetOverMap.class));
    }
}
