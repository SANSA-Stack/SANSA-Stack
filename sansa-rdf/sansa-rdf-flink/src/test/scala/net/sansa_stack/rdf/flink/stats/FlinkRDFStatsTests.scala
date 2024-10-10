package net.sansa_stack.rdf.flink.stats

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.jena.graph.Triple
import org.scalatest.{BeforeAndAfterAll, AnyFunSuite}

class FlinkRDFStatsTests
  extends AnyFunSuite
    with BeforeAndAfterAll {

  var env: ExecutionEnvironment = _

  var triples: DataSet[Triple] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    env = ExecutionEnvironment.createLocalEnvironment(4)

    val path = getClass.getResource("/data.nt").getPath

    triples = env.rdf(Lang.NTRIPLES)(path)
  }

  test("1. computing used classes should match") {
    val criteria = triples.statsUsedClasses()

    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing distinct subjects should match") {
    val criteria = triples.statsDistinctSubjects()

    val cnt = criteria.count()

    assert(cnt == 106)
  }

  test("computing Class Usage Count should match") {
    val criteria = triples.statsClassUsageCount()

    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Classes Defined should match") {
    val criteria = triples.statsClassesDefined()

    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Property Usage should match") {
    val criteria = triples.statsPropertyUsage()

    val cnt = criteria.count()

    assert(cnt == 24)
  }

  test("computing Distinct Entities should match") {
    val criteria = triples.statsDistinctEntities()

    val cnt = criteria.count()

    assert(cnt == 26)
  }

  test("computing Distinct Subjects should match") {
    val criteria = triples.statsDistinctSubjects()

    val cnt = criteria.count()

    assert(cnt == 106)
  }

  test("computing Distinct Objects should match") {
    val criteria = triples.statsDistinctObjects()

    val cnt = criteria.count()

    assert(cnt == 26)
  }

  test("computing Subject Vocabularies should match") {
    val criteria = triples.statsSubjectVocabularies()

    val cnt = criteria.count()

    assert(cnt == 5)
  }

  test("computing Predicate Vocabularies should match") {
    val criteria = triples.statsPredicateVocabularies()

    val cnt = criteria.count()

    assert(cnt == 3)
  }

  test("computing Object Vocabularies should match") {
    val criteria = triples.statsObjectVocabularies()

    val cnt = criteria.count()

    assert(cnt == 3)
  }

  test("computing Properties Defined should match") {
    val criteria = triples.statsPropertiesDefined()

    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Literals should match") {
    val criteria = triples.statsLiterals()

    val cnt = criteria.count()

    assert(cnt == 80)
  }

  test("computing Blanks as subject should match") {
    val criteria = triples.statsBlanksAsSubject()

    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Blanks as object should match") {
    val criteria = triples.statsBlanksAsObject()

    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Data types should match") {
    val criteria = triples.statsDataTypes()

    val cnt = criteria.count()

    assert(cnt == 5)
  }

  test("computing Langages should match") {
    val criteria = triples.statsLanguages()

    val cnt = criteria.count()

    assert(cnt == 1)
  }

  test("computing Labeled Subjects should match") {
    val criteria = triples.statsLabeledSubjects()

    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Same As should match") {
    val criteria = triples.statsSameAs()

    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Links between different namespaces should match") {
    val result = triples.statsLinks().collect().toSet

    val target = Set(
      ("http://commons.dbpedia.org/resource/File:", "http://exitinterview.biz/rarities/paidika/n12/", 1),
      ("http://commons.dbpedia.org/resource/File:De_Slegte,", "http://commons.dbpedia.org/resource/User:", 1),
      ("http://commons.dbpedia.org/resource/File:", "http://commons.dbpedia.org/resource/User:", 20))

    assert(result == target)
  }

}
