package org.sansa_stack.query.flink.sparqlify

import org.scalatest._

import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr}
import java.io.ByteArrayInputStream
import scala.collection.JavaConverters._
import net.sansa_stack.rdf.flink.partition.core.RdfPartitionUtilsFlink
import org.apache.jena.query.ResultSetFormatter

class TestRdfPartition extends FlatSpec {

  "A partitioner" should "support custom datatypes" in {

  }
}