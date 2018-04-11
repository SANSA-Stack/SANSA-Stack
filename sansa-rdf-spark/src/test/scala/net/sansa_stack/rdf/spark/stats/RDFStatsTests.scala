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

    val criteria = triples.statsDistinctSubjects()
    val cnt = criteria.count()

    assert(cnt == 3)
  }

  test("computing Used Classes should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsUsedClasses()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Class Usage Count should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsClassUsageCount()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Classes Defined should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsClassesDefined()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Property Usage should result in size 6") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsPropertyUsage()
    val cnt = criteria.count()

    assert(cnt == 6)
  }

  test("computing Distinct Entities should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsDistinctEntities()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Distinct Subjects should result in size 3") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsDistinctSubjects()
    val cnt = criteria.count()

    assert(cnt == 3)
  }

  test("computing Distinct Objects should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsDistinctObjects()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Subject Vocabularies should result in size 3") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsSubjectVocabularies()
    val cnt = criteria.count()

    assert(cnt == 3)
  }

  test("computing Predicate Vocabularies should result in size 5") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsPredicateVocabularies()
    val cnt = criteria.count()

    assert(cnt == 5)
  }

  test("computing Object Vocabularies should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsObjectVocabularies()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Properties Defined should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsPropertiesDefined()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

}