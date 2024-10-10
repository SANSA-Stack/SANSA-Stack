package net.sansa_stack.rdf.spark.partition

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.scalatest.funsuite.AnyFunSuite

class SemanticPartitionTests extends AnyFunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.partition._

  test("partitioning N-Triples file into Semantic Partition should match") {
    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val partitions = triples.partitionGraphAsSemantic()

    val size = partitions.count()

    assert(size == 34)
  }
}
