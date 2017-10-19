package net.sansa_stack.ml.spark.classification

import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }
import com.esotericsoftware.kryo.Kryo
import org.semanticweb.owlapi.model.OWLClass
import net.sansa_stack.ml.spark.classification.KB.KB
import org.semanticweb.owlapi.reasoner.structural.StructuralReasoner

/*
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