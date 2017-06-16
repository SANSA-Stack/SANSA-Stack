package org.sansa_stack.query.flink.sparqlify

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.google.common.collect.HashMultimap
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer
import net.sansa_stack.query.flink.sparqlify.{QueryExecutionFactorySparqlifyFlink, SparqlifyUtils3}
import net.sansa_stack.rdf.flink.partition.core.RdfPartitionUtilsFlink
import net.sansa_stack.rdf.partition.core.RdfPartitionDefault
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.jena.graph.Triple
import org.apache.jena.query.ResultSetFormatter
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.scalatest._
import net.sansa_stack.rdf.spark.io.JenaKryoSerializers._
import net.sansa_stack.rdf.spark.io.RestrictedExprSerializer
import org.aksw.jena_sparql_api.views.RestrictedExpr
import org.apache.jena.sparql.engine.binding.{Binding, BindingHashMap}

import scala.collection.JavaConverters._

class BindingSerializer extends Serializer[Binding] {
  override def write(kryo: Kryo, output: Output, obj: Binding) {
    //output.writeString(obj.getName)
  }

  override def read(kryo: Kryo, input: Input, objClass: Class[Binding]): Binding = {
    new BindingHashMap()
  }
}

class TestRdfPartition extends FlatSpec {

  "A partitioner" should "support custom datatypes" in {
    ExecutionEnvironment.getExecutionEnvironment.getConfig
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.addDefaultKryoSerializer(classOf[Binding], classOf[BindingSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[HashMultimap[_,_]], classOf[HashMultimapSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.sparql.core.Var], classOf[VarSerializer])
    //env.getConfig.registerTypeWithKryoSerializer(classOf[org.apache.jena.sparql.core.Var], classOf[VarSerializer])
    env.getConfig.registerKryoType(classOf[net.sansa_stack.rdf.partition.core.RdfPartitionDefault])
    env.getConfig.registerKryoType(classOf[Array[net.sansa_stack.rdf.partition.core.RdfPartitionDefault]])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node], classOf[NodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[Array[org.apache.jena.graph.Node]], classOf[NodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.sparql.core.Var], classOf[VarSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.sparql.expr.Expr], classOf[ExprSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node_Variable], classOf[VariableNodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node_Blank], classOf[NodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node_ANY], classOf[ANYNodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node_URI], classOf[NodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Node_Literal], classOf[NodeSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[org.apache.jena.graph.Triple], classOf[TripleSerializer])
    env.getConfig.addDefaultKryoSerializer(classOf[RestrictedExpr], classOf[RestrictedExprSerializer])
    env.getConfig.registerKryoType(classOf[Array[org.apache.jena.graph.Triple]])
    env.getConfig.registerKryoType(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])

    val flinkTable = TableEnvironment.getTableEnvironment(env)
    val triples = RDFDataMgr.createIteratorTriples(getClass.getResourceAsStream("/dbpedia-01.nt"), Lang.NTRIPLES, null).asScala
      //.map(t => RDFTriple(t.getSubject, t.getPredicate, t.getObject))
      .toList
    val dsAll: DataSet[Triple] = env.fromCollection(triples)
    val partition: Map[RdfPartitionDefault, DataSet[_ <: Product]] = RdfPartitionUtilsFlink.partitionGraph(dsAll)
    val views = SparqlifyUtils3.createSparqlSqlRewriter(env, flinkTable, partition)

//    val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(env, flinkTable, partition)
    val qef = new QueryExecutionFactorySparqlifyFlink(env, flinkTable, views)
    println(ResultSetFormatter.asText(qef.createQueryExecution("SELECT * { ?s ?p ?o }").execSelect()))

//    flinkTable.scan("deathPlace").printSchema();
//    val res = flinkTable.sql(
//      "SELECT CAST(s as VarChar)  FROM deathPlace"
//    )
//    res.toDataSet[Row].print()
    //println(flinkTable.explain(res))
    //ds.print()
    //env.execute()
  }
}