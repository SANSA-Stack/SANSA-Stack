package net.sansa_stack.rdf.spark.model.tensor

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.riot.Lang
import org.scalatest.funsuite.AnyFunSuite

class TensorTripleOpsTests extends AnyFunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model._

  test("converting RDD of triples into Tensor should match") {
    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val tensor = triples.asTensor()
    val size = tensor.count()

    assert(size == 442)
  }

  test("getting number of entities from RDD of triples should match") {
    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val num = triples.getNumEntities()

    assert(num == 24)
  }

  test("getting number of relations from RDD of triples should match") {
    val path = getClass.getResource("/rdf.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(path)

    val num = triples.getNumRelations()

    assert(num == 22)
  }

}
