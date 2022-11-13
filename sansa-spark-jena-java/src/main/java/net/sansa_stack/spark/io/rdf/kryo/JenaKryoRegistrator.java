package net.sansa_stack.spark.io.rdf.kryo;

import org.aksw.jenax.io.kryo.jena.JenaKryoRegistratorLib;
import org.aksw.jenax.io.kryo.jenax.JenaxKryoRegistratorLib;
import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

public class JenaKryoRegistrator
    implements KryoRegistrator
{
    @Override
    public void registerClasses(Kryo kryo) {
        JenaKryoRegistratorLib.registerClasses(kryo);
        JenaxKryoRegistratorLib.registerClasses(kryo);
    }
}
