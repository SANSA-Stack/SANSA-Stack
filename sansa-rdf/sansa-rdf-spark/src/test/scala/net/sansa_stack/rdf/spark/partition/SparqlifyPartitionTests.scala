package net.sansa_stack.rdf.spark.partition

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.scalatest.funsuite.AnyFunSuite

class SparqlifyPartitionTests extends AnyFunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.partition._

  test("partitioning N-Triples file into Sparqlify Partition (Vertical Partition) should match") {
    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val partitions = triples.partitionGraph()
    println(partitions.map(_._1).mkString("\n"))

    val size = partitions.size

    assert(size == 25)

  }
}
