package net.sansa_stack.spark.io.rdf.kryo;

import com.esotericsoftware.kryo.Kryo;
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
    }
}
