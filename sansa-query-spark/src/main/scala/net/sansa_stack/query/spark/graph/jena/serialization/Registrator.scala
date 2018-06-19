package net.sansa_stack.query.spark.graph.jena.serialization

import com.esotericsoftware.kryo.Kryo
import net.sansa_stack.query.spark.graph.jena.Ops
import net.sansa_stack.query.spark.graph.jena.expression.Expression
import net.sansa_stack.query.spark.graph.jena.util._
import org.apache.jena.graph.Node
import org.apache.jena.graph.Triple
import org.apache.spark.serializer.KryoRegistrator

/**
  * Register objects that serialized by the Kryo serializer.
  */
class Registrator extends KryoRegistrator{

  override def registerClasses(kryo: Kryo): Unit = {

    // Vertex and edge Attributes
    kryo.register(classOf[Node])
    kryo.register(classOf[Triple])

    // Query triple pattern
    kryo.register(classOf[TriplePattern])
    kryo.register(classOf[BasicGraphPattern])
    kryo.register(classOf[MatchCandidate])

    // Query processing
    kryo.register(classOf[Ops])
    kryo.register(classOf[Expression])

    // Result
    kryo.register(classOf[Result[Node]])
  }
}
