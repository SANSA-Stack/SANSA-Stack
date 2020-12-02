package net.sansa_stack.query.spark.ontop

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.JavaSerializer
import org.apache.spark.serializer.KryoRegistrator
import uk.ac.manchester.cs.owl.owlapi.OWLOntologyImpl
import uk.ac.manchester.cs.owl.owlapi.concurrent.ConcurrentOWLOntologyImpl;

class KryoRegistratorOntop
  extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[OWLOntologyImpl], new JavaSerializer())
		kryo.register(classOf[ConcurrentOWLOntologyImpl], new JavaSerializer())
  }

}