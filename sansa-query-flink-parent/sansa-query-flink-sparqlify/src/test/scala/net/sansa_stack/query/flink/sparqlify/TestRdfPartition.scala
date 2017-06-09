package org.sansa_stack.query.flink.sparqlify

import org.scalatest._
import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr}
import java.io.ByteArrayInputStream

import net.sansa_stack.rdf.flink.data.RDFGraphLoader
import net.sansa_stack.rdf.flink.model.RDFTriple

import scala.collection.JavaConverters._
import net.sansa_stack.rdf.flink.partition.core.RdfPartitionUtilsFlink
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.jena.query.ResultSetFormatter

class TestRdfPartition extends FlatSpec {

  "A partitioner" should "support custom datatypes" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val triples = RDFDataMgr.createIteratorTriples(getClass.getResourceAsStream("/dbpedia-01.nt"), Lang.NTRIPLES, null).asScala
        //.map(t => RDFTriple(t.getSubject, t.getPredicate, t.getObject))
      .toList
    val ds: DataSet[Triple] = env.fromCollection(triples)
    val partition = RdfPartitionUtilsFlink.partitionGraph(ds)
    partition.foreach(u => print(u._1))
    ds.print()
    //env.execute()
  }
}