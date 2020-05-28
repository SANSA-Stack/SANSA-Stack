package net.sansa_stack.query.spark.graph.jena.serialization

import com.esotericsoftware.kryo.Kryo
import net.sansa_stack.query.spark.graph.jena._
import net.sansa_stack.query.spark.graph.jena.expression.Expression
import net.sansa_stack.query.spark.graph.jena.serialization.JenaKryoSerializers.{DatasetSerializer, QuerySerializer}
import net.sansa_stack.query.spark.graph.jena.util._
import org.apache.jena.graph.Node
import org.apache.jena.query.Query
import org.apache.jena.sparql.core.DatasetImpl
import org.apache.spark.serializer.KryoRegistrator

/**
  * Register objects that serialized by the Kryo serializer.
  */
class Registrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {

    // Jena's Query class
    kryo.register(classOf[Query], new QuerySerializer)
    kryo.register(classOf[DatasetImpl], new DatasetSerializer)


    // Triple pattern
    kryo.register(classOf[TriplePattern])
    kryo.register(classOf[BasicGraphPattern])
    kryo.register(classOf[MatchCandidate])

    // Parser
    kryo.register(classOf[SparqlParser])
    kryo.register(classOf[ExprParser])

    // Query processing
    kryo.register(classOf[Ops])
    kryo.register(classOf[Expression])

    // Result
    kryo.register(classOf[Result[Node]])
  }
}
