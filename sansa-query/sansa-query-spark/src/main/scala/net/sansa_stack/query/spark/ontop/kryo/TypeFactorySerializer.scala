package net.sansa_stack.query.spark.ontop.kryo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import it.unibz.inf.ontop.model.`type`.TypeFactory
import net.sansa_stack.query.spark.ontop.OntopConnection

class TypeFactorySerializer(ontopSessionID: String)
  extends Serializer[TypeFactory](false, true) {


  override def write(kryo: Kryo, output: Output, obj: TypeFactory): Unit = {
    kryo.writeClass(output, obj.getClass)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[TypeFactory]): TypeFactory = {
    kryo.readClass(input)
    OntopConnection.configs.get(Option(ontopSessionID).getOrElse(OntopConnection.configs.head._1)).get.getTypeFactory
  }
}

