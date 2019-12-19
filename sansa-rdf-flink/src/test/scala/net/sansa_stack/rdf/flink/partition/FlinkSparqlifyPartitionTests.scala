package net.sansa_stack.rdf.flink.partition

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class FlinkSparqlifyPartitionTests extends FunSuite {

  val env = ExecutionEnvironment.getExecutionEnvironment

  test("partitioning N-Triples file into Sparqlify Partition (Vertical Partition) should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val partitions = triples.partitionGraph()

    val size = partitions.size

    assert(size == 28)

  }

}
