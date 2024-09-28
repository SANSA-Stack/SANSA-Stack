package net.sansa_stack.rdf.spark.io

import com.esotericsoftware.kryo.Kryo
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer
import org.aksw.jenax.io.kryo.jena.JenaKryoRegistratorLib
import net.sansa_stack.rdf.common.partition.core.RdfPartitionStateDefault
import net.sansa_stack.rdf.spark.io.JenaKryoRegistrator.register
import org.aksw.jenax.io.kryo.jenax.JenaxKryoRegistratorLib
import org.apache.spark.serializer.KryoRegistrator

object JenaKryoRegistrator {
  def register(kryo: Kryo): Unit = {
    JenaKryoRegistratorLib.registerClasses(kryo);
    JenaxKryoRegistratorLib.registerClasses(kryo)

    HashMultimapSerializer.registerSerializers(kryo)

    // Partitioning
    kryo.register(classOf[RdfPartitionStateDefault])
    kryo.register(classOf[Array[RdfPartitionStateDefault]])

    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
  }
}

/**
 * Created by nilesh on 01/06/2016.
 *
 * @author Nilesh
 * @author Claus Stadler
 * @author Lorenz Buehmann
 */
class JenaKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    register(kryo)
  }
}
