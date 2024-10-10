package net.sansa_stack.rdf.flink.partition

import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.funsuite.AnyFunSuite

class FlinkSparqlifyPartitionTests extends AnyFunSuite {

  val env = ExecutionEnvironment.getExecutionEnvironment

  /* FIXME This test is broken due to change in the partition object:
      This type (interface scala.collection.immutable.Set[String]) cannot be used as key. */
  /*
  test("partitioning N-Triples file into Sparqlify Partition (Vertical Partition) should match") {
    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val partitions = triples.partitionGraph()

    val size = partitions.size

    assert(size == 28)

  }
  */

}
