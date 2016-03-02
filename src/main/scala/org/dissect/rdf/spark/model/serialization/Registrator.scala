package org.dissect.rdf.spark.model.serialization
import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }
import com.esotericsoftware.kryo.Kryo
import org.dissect.rdf.spark.model._
import org.dissect.rdf.spark.utils.NTriplesParser
/*
 * Class for serialization by the Kryo serializer.
 */
class Registrator extends SparkKryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    // model
    kryo.register(classOf[Triple])
    kryo.register(classOf[TripleRDD])

    // utils
    //kryo.register(classOf[NTriplesParser])

  }
}