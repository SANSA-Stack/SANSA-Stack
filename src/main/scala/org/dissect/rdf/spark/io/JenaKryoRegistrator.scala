package org.dissect.rdf.spark.io

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.dissect.rdf.spark.io.JenaKryoSerializers._
import org.dissect.rdf.spark.model.{JenaSparkRDD, TripleRDD}

/**
  * Created by nilesh on 01/06/2016.
  */
class JenaKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Array[org.apache.jena.graph.Node]], new NodeArraySerializer)
    kryo.register(classOf[org.apache.jena.graph.Node_Blank], new BlankNodeSerializer)
    kryo.register(classOf[org.apache.jena.graph.Node_ANY], new ANYNodeSerializer)
    kryo.register(classOf[org.apache.jena.graph.Node_Variable], new VariableNodeSerializer)
    kryo.register(classOf[org.apache.jena.graph.Node_URI], new URINodeSerializer)
    kryo.register(classOf[org.apache.jena.graph.Node_Literal], new LiteralNodeSerializer)
    kryo.register(classOf[org.apache.jena.graph.Triple], new TripleSerializer)
    kryo.register(classOf[Array[org.apache.jena.graph.Triple]])
    kryo.register(Class.forName("org.dissect.rdf.spark.model.SparkRDDGraphOps$$anonfun$findGraph$1"))
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[org.dissect.rdf.spark.model.TripleRDD])
  }
}
