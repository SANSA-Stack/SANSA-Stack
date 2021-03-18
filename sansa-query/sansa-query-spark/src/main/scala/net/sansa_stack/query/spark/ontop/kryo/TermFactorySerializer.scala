package net.sansa_stack.query.spark.ontop.kryo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import it.unibz.inf.ontop.model.term.TermFactory
import net.sansa_stack.query.spark.ontop.OntopConnection

class TermFactorySerializer(ontopSessionID: String)
  extends Serializer[TermFactory](false, true) {


  override def write(kryo: Kryo, output: Output, obj: TermFactory): Unit = {
    kryo.writeClass(output, obj.getClass)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[TermFactory]): TermFactory = {
    kryo.readClass(input)
    OntopConnection.configs.get(Option(ontopSessionID).getOrElse(OntopConnection.configs.head._1)).get.getTermFactory
  }
}

