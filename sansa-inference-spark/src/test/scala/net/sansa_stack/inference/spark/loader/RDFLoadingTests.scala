package net.sansa_stack.inference.spark.loader

import scala.io.Source

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

/**
  * @author Lorenz Buehmann
  */
class RDFLoadingTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.inference.spark.data.loader.sql.rdf._

  test("loading N-Triples from disk into DataFrame") {
    val sqlCtx = sqlContext

    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = sqlCtx.read.rdf(lang)(path)

    val cnt = triples.count()
    println(cnt)
  }

}
