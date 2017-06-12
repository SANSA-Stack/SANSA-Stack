package org.sansa_stack.query.flink.sparqlify

import org.scalatest._
import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr}
import java.io.ByteArrayInputStream
import scala.reflect.runtime.universe.typeOf

import net.sansa_stack.rdf.flink.data.RDFGraphLoader
import net.sansa_stack.rdf.flink.model.RDFTriple

import scala.collection.JavaConverters._
import net.sansa_stack.rdf.flink.partition.core.RdfPartitionUtilsFlink
import net.sansa_stack.rdf.partition.core.{RdfPartition, RdfPartitionDefault}
import net.sansa_stack.rdf.partition.schema._
import net.sansa_stack.rdf.partition.sparqlify.SparqlifyUtils2
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.apache.jena.query.ResultSetFormatter

class TestRdfPartition extends FlatSpec {

  "A partitioner" should "support custom datatypes" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tblEnv = TableEnvironment.getTableEnvironment(env)
    val triples = RDFDataMgr.createIteratorTriples(getClass.getResourceAsStream("/dbpedia-01.nt"), Lang.NTRIPLES, null).asScala
        //.map(t => RDFTriple(t.getSubject, t.getPredicate, t.getObject))
      .toList
    val dsAll: DataSet[Triple] = env.fromCollection(triples)
    val partition: Map[RdfPartitionDefault, DataSet[_ <: Product]] = RdfPartitionUtilsFlink.partitionGraph(dsAll)
    val tables = partition.foreach { case (p, ds) => {
      val vd = SparqlifyUtils2.createViewDefinition(p)
      println(vd.getName)
      val q = p.layout.schema
      q match {
        case q if q =:= typeOf[SchemaStringLong] =>
          tblEnv.registerDataSet(vd.getName, ds.map { r => r.asInstanceOf[SchemaStringLong] })
        case q if q =:= typeOf[SchemaStringString] =>
          tblEnv.registerDataSet(vd.getName, ds.map { r => r.asInstanceOf[SchemaStringString] })
        case q if q =:= typeOf[SchemaStringStringType] =>
          tblEnv.registerDataSet(vd.getName, ds.map { r => r.asInstanceOf[SchemaStringStringType] })
        case q if q =:= typeOf[SchemaStringDouble] =>
          tblEnv.registerDataSet(vd.getName, ds.map { r => r.asInstanceOf[SchemaStringDouble] })
        case q if q =:= typeOf[SchemaStringStringLang] =>
          tblEnv.registerDataSet(vd.getName, ds.map { r => r.asInstanceOf[SchemaStringStringLang] })
      }
      (p, vd.getName)
    }
    }
    tblEnv.scan("deathPlace").printSchema();
    val res = tblEnv.sql(
      "SELECT s FROM deathPlace"
    )
    res.toDataSet[Row].print()
    //println(tblEnv.explain(res))



    //ds.print()
    //env.execute()
  }
}