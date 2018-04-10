package net.sansa_stack.rdf.spark.stats

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._

class RDFStatsTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.stats._

  test("computing distinct subjects should result in size 3") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val distinct_subjects = triples.statsDistinctSubjects()

    assert(distinct_subjects.count() == 3)
  }

}