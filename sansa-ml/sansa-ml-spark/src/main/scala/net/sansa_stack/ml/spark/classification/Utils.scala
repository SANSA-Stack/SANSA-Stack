package net.sansa_stack.ml.spark.classification

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }
import org.semanticweb.owlapi.model.OWLClass
import org.semanticweb.owlapi.reasoner.structural.StructuralReasoner

import net.sansa_stack.ml.spark.classification.KB.KB

/**
 * Class for serialization by the Kryo serializer.
 */
class Registrator extends SparkKryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    // model
    kryo.register(classOf[OWLClass])
    kryo.register(classOf[StructuralReasoner])
    kryo.register(classOf[net.sansa_stack.ml.spark.classification.KB.KB])
  }
}
