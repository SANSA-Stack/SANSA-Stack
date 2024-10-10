package net.sansa_stack.inference.spark.loader

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import org.scalatest.funsuite.AnyFunSuite

/**
  * Tests for loading triples from either N-Triples or Turtle files into a DataFrame.
  *
  * @author Lorenz Buehmann
  */
class RDFLoadingTests extends AnyFunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.io._

  test("loading N-Triples file into DataFrame with REGEX parsing mode should result in 9 triples") {
    val sqlCtx = sqlContext

    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = sqlCtx.read.rdf(lang)(path)

    val cnt = triples.count()
    assert(cnt == 9)
  }

  test("loading Turtle file into DataFrame should result in 12 triples") {
    val sqlCtx = sqlContext

    val path = getClass.getResource("/loader/data.ttl").getPath
    val lang: Lang = Lang.TURTLE

    val triples = sqlCtx.read.rdf(lang)(path)

    val cnt = triples.count()
    assert(cnt == 12)
  }

}
