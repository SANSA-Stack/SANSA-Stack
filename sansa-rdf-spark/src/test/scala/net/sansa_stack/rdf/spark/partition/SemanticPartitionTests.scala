package net.sansa_stack.rdf.spark.partition

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._

class SemanticPartitionTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.partition._

  test("partitioning N-Triples file into Semantic Partition should result in size 37") {
    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val partitions = triples.partitionGraphAsSemantic()

    val size = partitions.count()

    assert(size == 37)

  }
}