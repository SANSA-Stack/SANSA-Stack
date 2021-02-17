package net.sansa_stack.rdf.spark.io

import com.esotericsoftware.kryo.Kryo
import net.sansa_stack.rdf.common.kryo.jena.JenaKryoRegistratorLib
import org.apache.spark.serializer.KryoRegistrator

/**
 * Created by nilesh on 01/06/2016.
 */
class JenaKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    JenaKryoRegistratorLib.registerClasses(kryo);

    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
  }
}
