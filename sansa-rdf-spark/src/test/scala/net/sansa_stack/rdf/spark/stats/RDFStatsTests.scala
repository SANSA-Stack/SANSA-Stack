package net.sansa_stack.rdf.spark.stats

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.NodeFactory
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class RDFStatsTests extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.stats._

  test("computing distinct subjects should result in size 4") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsDistinctSubjects()
    val cnt = criteria.count()

    assert(cnt == 4)
  }

  test("computing Used Classes should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsUsedClasses()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Class Usage Count should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsClassUsageCount()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Classes Defined should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsClassesDefined()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Property Usage should result in size 6") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsPropertyUsage()
    val cnt = criteria.count()

    assert(cnt == 7)
  }

  test("computing Distinct Entities should result in size 9") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsDistinctEntities()
    val cnt = criteria.count()

    assert(cnt == 12)
  }

  test("computing Distinct Subjects should result in size 3") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsDistinctSubjects()
    val cnt = criteria.count()

    assert(cnt == 4)
  }

  test("computing Distinct Objects should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsDistinctObjects()
    val cnt = criteria.count()

    assert(cnt == 1)
  }

  test("computing Subject Vocabularies should result in size 3") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsSubjectVocabularies()
    val cnt = criteria.count()

    assert(cnt == 4)
  }

  test("computing Predicate Vocabularies should result in size 5") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsPredicateVocabularies()
    val cnt = criteria.count()

    assert(cnt == 6)
  }

  test("computing Object Vocabularies should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsObjectVocabularies()
    val cnt = criteria.count()

    assert(cnt == 1)
  }

  test("computing Properties Defined should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsPropertiesDefined()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Literals should result in size 7") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsLiterals()
    val cnt = criteria.count()

    assert(cnt == 7)
  }

  test("computing Blanks as subject should result in size 2") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsBlanksAsSubject()
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("computing Blanks as object should result in size 2") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsBlanksAsObject()
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("computing Data types should result in size 4") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsDatatypes()
    val cnt = criteria.count()

    assert(cnt == 4)
  }

  test("computing Langages should result in size 2") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsLanguages()
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("computing Avgerage Typed String Length should result in size 39.0") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val cnt = triples.statsAvgTypedStringLength()

    assert(cnt == (19 + 19 + 79) / 3)
  }

  test("computing Avgerage UnTyped String Length should result in 25.0") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val cnt = triples.statsAvgUntypedStringLength()

    assert(cnt == 25.0)
  }

  test("computing Typed Subjects should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsTypedSubjects()
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Labeled Subjects should result in size 2") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsLabeledSubjects()
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("computing Same As should result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsSameAs()
    val cnt = criteria.count()

    assert(cnt == 1)
  }

  test("computing Links between different namespaces should match the expected result") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val result = triples.statsLinks().collect().toSet

    val target = Set(
      ("http://namespace1.org/show/218", "http://namespace2.org/show/218", 1))

    assert(result == target)
  }

  test("computing Subject Vocabularies result in size 3") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsSubjectVocabularies()
    val cnt = criteria.count()

    assert(cnt == 4)
  }

  test("computing Predicate Vocabularies result in size 5") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsPredicateVocabularies()
    val cnt = criteria.count()

    assert(cnt == 6)
  }

  test("computing Object Vocabularies result in size 0") {
    val path = getClass.getResource("/loader/data.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsObjectVocabularies()
    val cnt = criteria.count()

    assert(cnt == 1)
  }

  test("4 - computing Class Hierarchy Depth should match") {
    val path = getClass.getResource("/stats/4_class_hierarchy.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.classHierarchyDepth()

    val target = Set(
      (NodeFactory.createURI("http://example.org/D"), 4),
      (NodeFactory.createURI("http://example.org/E"), 3),
      (NodeFactory.createURI("http://example.org/C"), 3),
      (NodeFactory.createURI("http://example.org/B"), 2),
      (NodeFactory.createURI("http://example.org/A"), 1))

    val result = criteria.collect().toSet

    assert(target == result)
  }

  test("28 - computing Max Value Per Property should match") {
    val path = getClass.getResource("/stats/28_max_per_property.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val target = Set(
      (NodeFactory.createURI("http://example.org/dtp"), NodeFactory.createLiteral("2004-04-12T16:20:00", XSDDatatype.XSDdateTime)),
      (NodeFactory.createURI("http://example.org/dp1"), NodeFactory.createLiteral("123.0", XSDDatatype.XSDdouble)),
      (NodeFactory.createURI("http://example.org/ip2"), NodeFactory.createLiteral("1111", XSDDatatype.XSDint)),
      (NodeFactory.createURI("http://example.org/dp2"), NodeFactory.createLiteral("1111.0", XSDDatatype.XSDdouble)),
      (NodeFactory.createURI("http://example.org/ip1"), NodeFactory.createLiteral("123", XSDDatatype.XSDint)))

    val result = triples.statsMaxPerProperty().collect().toSet

    assert(target == result)
  }

  test("29 - computing Avg Value Per Property should match") {
    val path = getClass.getResource("/stats/28_max_per_property.nt").getPath

    val triples = spark.rdf(Lang.NTRIPLES)(path)

    val target = Set(
      (NodeFactory.createURI("http://example.org/dp1"), (10.0 + (-30.0) + 123.0) / 3),
      (NodeFactory.createURI("http://example.org/dp2"), (1111.0 + (-90.0) + 2.0) / 3),
      (NodeFactory.createURI("http://example.org/ip1"), (10 + (-30) + 123) / 3.0),
      (NodeFactory.createURI("http://example.org/ip2"), (1111 + (-90) + 2) / 3.0))

    val result = triples.statsAvgPerProperty().collect().toSet

    assert(target == result)
  }
}
