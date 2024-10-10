package net.sansa_stack.query.spark.hdt

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite


class HDTTests extends AnyFunSuite with DataFrameSuiteBase {

  JenaSystem.init

  import net.sansa_stack.query.spark._

  var hdt_triples: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val input = getClass.getResource("/sansa-sparql-ts/bsbm/bsbm-sample.nt").getPath
    val triples = spark.rdf(Lang.NTRIPLES)(input)
    hdt_triples = triples.asHDT().cache()
  }

  test("result of running `SIMPLE SELECT` should match") {
    val query =
      """
        |SELECT ?S ?O ?P  WHERE { ?S ?P ?O }
      """.stripMargin

    val result = hdt_triples.sparqlHDT(query)

    val size = result.count()

    assert(size == 40377)
  }

  test("result of running `Typed Predicate` should match") {
    val query =
      """
        |SELECT ?S ?O ?P WHERE { ?S <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?O .  }
      """.stripMargin

    val result = hdt_triples.sparqlHDT(query)

    val size = result.count()

    assert(size == 4474)
  }

  test("result of running `FILTER` should match") {
    val query =
      """
        |SELECT ?S ?O ?P WHERE { ?S ?P ?O . FILTER ( STRLEN(?S) >= 80 ) . }
      """.stripMargin

    val result = hdt_triples.sparqlHDT(query)

    val size = result.count()

    assert(size == 35167)
  }

}
