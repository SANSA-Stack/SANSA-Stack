package net.sansa_stack.rdf.spark.stats

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

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

  test("computing Literals should result in size 7") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsLiterals()
    val cnt = criteria.count()

    assert(cnt == 7)
  }

  test("computing Blanks as subject should result in size 2") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsBlanksAsSubject()
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("computing Blanks as object should result in size 2") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsBlanksAsObject()
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("computing Data types should result in size 4") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsDatatypes()
    val cnt = criteria.count()

    assert(cnt == 4)
  }

  test("computing Langages should result in size 2") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsLanguages()
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("computing Avgerage Typed String Length should result in size 42.666666666666664") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val cnt = triples.statsAvgTypedStringLength()

    assert(cnt == 42.666666666666664)
  }

  test("computing Avgerage UnTyped String Length should result in 25.0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val cnt = triples.statsAvgUntypedStringLength()

    assert(cnt == 25.0)
  }

  test("computing Typed Subjects should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsTypedSubjects()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Labeled Subjects should result in size 2") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsLabeledSubjects()
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("computing Same As should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsSameAs()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Links should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsLinks()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Subject Vocabularies result in size 3") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsSubjectVocabularies()
    val cnt = criteria.count()

    assert(cnt == 3)
  }

  test("computing Predicate Vocabularies result in size 5") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsPredicateVocabularies()
    val cnt = criteria.count()

    assert(cnt == 5)
  }

  test("computing Object Vocabularies result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = spark.rdf(lang, allowBlankLines = true)(path)

    val criteria = triples.statsObjectVocabularies()
    val cnt = criteria.count()

    assert(cnt == 0)
  }
}
