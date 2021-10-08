package net.sansa_stack.spark.io.rdf.kryo;

import com.esotericsoftware.kryo.Kryo;
import net.sansa_stack.kryo.jena.JenaKryoRegistratorLib;
import org.apache.spark.serializer.KryoRegistrator;

public class JenaKryoRegistrator
    implements KryoRegistrator
{
    @Override
    public void registerClasses(Kryo kryo) {
        JenaKryoRegistratorLib.registerClasses(kryo);
    }
}
