package net.sansa_stack.rdf.spark.io

import com.esotericsoftware.kryo.Kryo
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer
import net.sansa_stack.rdf.common.kryo.jena.JenaKryoSerializers._
import net.sansa_stack.rdf.spark.kryo.jena.KryoRegistratorRDFNode
import org.apache.jena.query.Query
import org.apache.jena.rdf.model.impl.ModelCom
import org.apache.jena.sparql.core.DatasetImpl
import org.apache.spark.serializer.KryoRegistrator

/**
 * Created by nilesh on 01/06/2016.
 */
class JenaKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {

    // TODO Exprs and Query should go to query layer
    // The NodeValue list should be complete for jena 3.17.0
    // kryo.register(classOf[org.apache.jena.sparql.expr.NodeValue], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.nodevalue.NodeValueBoolean], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.nodevalue.NodeValueDecimal], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.nodevalue.NodeValueDouble], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.nodevalue.NodeValueDT], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.nodevalue.NodeValueDuration], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.nodevalue.NodeValueFloat], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.nodevalue.NodeValueInteger], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.nodevalue.NodeValueLang], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.nodevalue.NodeValueNode], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.nodevalue.NodeValueSortKey], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.nodevalue.NodeValueString], new ExprSerializer)

    // This list is incomplete - use class path scanning?
    // However this would probably slows down startup time alot for that number of classes...
    kryo.register(classOf[org.apache.jena.sparql.expr.ExprVar], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.E_Equals], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.E_BNode], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.E_Datatype], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.E_IRI], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.E_Datatype], new ExprSerializer)
    kryo.register(classOf[org.apache.jena.sparql.expr.Expr], new ExprSerializer)


    kryo.register(classOf[Query], new QuerySerializer)

    HashMultimapSerializer.registerSerializers(kryo);

    // Partitioning
    kryo.register(classOf[net.sansa_stack.rdf.common.partition.core.RdfPartitionStateDefault])
    kryo.register(classOf[Array[net.sansa_stack.rdf.common.partition.core.RdfPartitionStateDefault]])

    kryo.register(classOf[org.apache.jena.graph.Node], new NodeSerializer)
    kryo.register(classOf[Array[org.apache.jena.graph.Node]], new NodeArraySerializer)
    kryo.register(classOf[org.apache.jena.sparql.core.Var], new VarSerializer)
    kryo.register(classOf[org.apache.jena.graph.Node_Variable], new VariableNodeSerializer)
    kryo.register(classOf[org.apache.jena.graph.Node_Blank], new NodeSerializer)
    kryo.register(classOf[org.apache.jena.graph.Node_ANY], new ANYNodeSerializer)
    kryo.register(classOf[org.apache.jena.graph.Node_URI], new NodeSerializer)
    kryo.register(classOf[org.apache.jena.graph.Node_Literal], new NodeSerializer)
    kryo.register(classOf[org.apache.jena.graph.Triple], new TripleSerializer)
    kryo.register(classOf[Array[org.apache.jena.graph.Triple]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])


    kryo.register(classOf[ModelCom], new ModelSerializer)
    kryo.register(classOf[DatasetImpl], new DatasetSerializer)


    KryoRegistratorRDFNode.registerClassesActual(kryo)

  }
}
