package net.sansa_stack.owl.spark.stats

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.semanticweb.owlapi.apibinding.OWLManager

import net.sansa_stack.owl.spark.owl
import net.sansa_stack.owl.spark.owl._
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

class OWLStatsTests extends FunSuite with SharedSparkContext {

  lazy val spark: SparkSession = SparkSession.builder().appName(sc.appName).master(sc.master)
    .config("spark.kryo.registrator",
      "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
    .getOrCreate()

  val df = OWLManager.getOWLDataFactory

  val syntax: owl.Syntax.Value = Syntax.FUNCTIONAL

  val filePath: String = this.getClass.getClassLoader.getResource("ont_functional.owl").getPath

  var _rdd: OWLAxiomsRDD = null

  def rdd: OWLAxiomsRDD = {
    if (_rdd == null) {
      _rdd = spark.owl(syntax)(filePath)
      _rdd.cache()
    }
    _rdd
  }

  test("Criteria 1. Computing Used Classes should result in size 1") {

    val criteria = new UsedClasses(rdd, spark).filter()
    val cnt = criteria.count()

    assert(cnt == 1)
  }

  test("Criteria 2. Computing Used Classes Count should result in size 1") {

    val criteria = new UsedClassesCount(rdd, spark).action()
    val cnt = criteria.count()

    assert(cnt == 1)
  }

  /** Two axioms should be extracted
    * Declaration(Annotation(foo:ann "some annotation") Class(bar:Cls1))
    * Declaration(Class(bar:Cls2))
   */

  test("Criteria 3. Computing Classes Defined should result in size 2") {

    val criteria = new DefinedClasses(rdd, spark).action()
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("Criteria 4. Computing Class Hierarchy Depth should match") {

    val prefix = "http://ex.com/bar#"

    val path: String = this.getClass.getClassLoader.getResource("hierarchy_depth.owl").getPath

    val axioms = spark.owl(syntax)(path)

    val criteria = new OWLStats(spark).getClassHierarchyDepth(axioms)

    val result = criteria.collect().toSet

    val target = Set((df.getOWLClass(prefix + "UnionCls").toString, 0),
                     (df.getOWLClass(prefix + "Cls1").toString, 1),
                     (df.getOWLClass(prefix + "Cls2").toString, 1),
                     (df.getOWLClass(prefix + "Cls3").toString, 2),
                     (df.getOWLClass(prefix + "Cls4").toString, 3))

    assert(target == result)
  }

  test("Criteria 5a. Computing Used Data Properties should result in size 1") {

    val criteria = new UsedDataProperties(rdd, spark).filter()
    val cnt = criteria.count()

    assert(cnt == 1)
  }

  test("Criteria 5b. Computing Used Object Properties should result in size 1") {

    val criteria = new UsedObjectProperties(rdd, spark).filter()
    val cnt = criteria.count()

    assert(cnt == 1)
  }

  test("Criteria 6. Computing Property Usage Distinct per Subject should result in size 5") {

    val criteria = new OWLStats(spark).getPropertyUsageDistinctPerSubject(rdd)
    val cnt = criteria.count()

    assert(cnt == 5)
  }

  test("Criteria 7. Computing Property Usage Distinct per Object should result in size 6") {

    val criteria = new OWLStats(spark).getPropertyUsageDistinctPerObject(rdd)
    val cnt = criteria.count()

    assert(cnt == 6)
  }

  test("Criteria 10. Computing Outdegree should result in size 16") {

    val criteria = new OWLStats(spark).getOutdegree(rdd)
    val cnt = criteria.count()

    assert(cnt == 16)
  }

  test("Criteria 11. Computing Indegree should result in size 23") {

    val criteria = new OWLStats(spark).getInDegree(rdd)
    val cnt = criteria.count()

    assert(cnt == 23)
  }

  test("Criteria 12a. Computing Data Property Hierarchy Depth should match") {

    val prefix = "http://ex.com/bar#"

    val path: String = this.getClass.getClassLoader.getResource("hierarchy_depth.owl").getPath

    val axioms = spark.owl(syntax)(path)

    val criteria = new OWLStats(spark).getDataPropertyHierarchyDepth(axioms)

    val result = criteria.collect().toSet

    val target = Set((df.getOWLDataProperty(prefix + "dataProp1").toString, 0),
                     (df.getOWLDataProperty(prefix + "dataProp2").toString, 1),
                     (df.getOWLDataProperty(prefix + "dataProp3").toString, 2),
                     (df.getOWLDataProperty(prefix + "dataProp4").toString, 3),
                     (df.getOWLDataProperty(prefix + "dataProp5").toString, 1))

    assert(target == result)
  }

  test("Criteria 12b. Computing Object Property Hierarchy Depth should match") {

    val prefix = "http://ex.com/bar#"

    val path: String = this.getClass.getClassLoader.getResource("hierarchy_depth.owl").getPath

    val axioms = spark.owl(syntax)(path)

    val criteria = new OWLStats(spark).getObjectPropertyHierarchyDepth(axioms)

    val result = criteria.collect().toSet

    val target = Set((df.getOWLObjectProperty(prefix + "objProp1").toString, 0),
      (df.getOWLObjectProperty(prefix + "objProp2").toString, 1),
      (df.getOWLObjectProperty(prefix + "objProp3").toString, 2),
      (df.getOWLObjectProperty(prefix + "objProp4").toString, 3),
      (df.getOWLObjectProperty(prefix + "objProp5").toString, 1))

    assert(target == result)
  }

  test("Criteria 13. Computing SubClass Usage should result in size 1") {

    val criteria = new OWLStats(spark).getSubclassUsage(rdd)
    assert(criteria == 1)
  }

  test("Criteria 14. Computing axioms count should result in size 67") {

    val criteria = new OWLStats(spark).getAxiomCount(rdd)
    assert(criteria == 67)
  }

  test("Criteria 15. Computing Mentioned Entities Count should result in size 46") {

    val criteria = new OWLStats(spark).getOWLMentionedEntities(rdd)
    assert(criteria == 46)
  }

  test("Criteria 16. Computing Distinct Entities Count should result in size 25") {

    val criteria = new OWLStats(spark).getOWLDistinctEntities(rdd)
    val cnt = criteria.count()

    assert(cnt == 25)
  }

  test("Criteria 17. Computing Literals Count should result in 3") {

    // which are "ABCD", "BCDE", and "23"
    val criteria = new OWLStats(spark).getLiteralAssertionsCount(rdd)
    assert(criteria == 3)
  }

  test("Criteria 20. Computing Datatypes Histogram for Literals should result in size 2") {

    val criteria = new OWLStats(spark).getDatatypesHistogram(rdd)
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("Criteria 21. Computing Language Histogram for Literals should result in size 0") {

    val criteria = new OWLStats(spark).getLanguagesHistogram(rdd)
    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("Criteria 22. Computing Average Typed String Length should result in 4.0") {

    val criteria = new OWLStats(spark).getAvgTypedStringLength(rdd)

    assert(criteria == (4 + 4) / 2)
  }

  test("Criteria 23. Computing Average Untyped String Length should result in 0.0") {

    val criteria = new OWLStats(spark).getAvgUntypedStringLength(rdd)

    assert(criteria == 0.0)
  }

  test("Criteria 24. Computing Typed Subject should result in size 14") {

    val criteria = new OWLStats(spark).getTypedSubject(rdd)
    val cnt = criteria.count()

    assert(cnt == 14)
  }

  test("Criteria 25. Computing Labeled Subject should result in size 1") {

    val criteria = new OWLStats(spark).getLabeledSubjects(rdd)
    val cnt = criteria.count()

    assert(cnt == 1)
  }

  test("Criteria 26. Computing SameAs axioms count should result in 1") {

    val criteria = new OWLStats(spark).getSameAsAxiomsCount(rdd)
    assert(criteria == 1)
  }

  test("Criteria 27. Computing Links between different namespaces should match") {

    val criteria = new OWLStats(spark).getNamespaceLinks(rdd)

    val result = criteria.collect().toSet

    val target = Set(("http://ex.com/bar#", "http://www.w3.org/2002/07/owl#", 10),
                     ("http://ex.com/bar#", "http://www.w3.org/2000/01/rdf-schema#", 2),
                     ("http://ex.com/foo#", "http://www.w3.org/2001/XMLSchema#", 2),
                     ("http://ex.com/foo#", "http://ex.com/bar#", 1),
                     ("http://ex.com/foo#", "http://www.w3.org/2002/07/owl#", 2))

    assert(target == result)
  }

  test("Criteria 29. Computing Max Value Per Property should match") {

    val prefix = "http://ex.com/bar#"

    val path: String = this.getClass.getClassLoader.getResource("max_per_numericDatatype.owl").getPath

    val axioms = spark.owl(syntax)(path)

    val criteria = new OWLStats(spark).getMaxPerNumericDatatypeProperty(axioms)

    val result = criteria.collect().toSet

    val target = Set((df.getOWLDataProperty(prefix + "dataProp1"), 2320.0),
                     (df.getOWLDataProperty(prefix + "dataProp2"), 70.0),
                     (df.getOWLDataProperty(prefix + "dataProp3"), 123.0),
                     (df.getOWLDataProperty(prefix + "dataProp4"), 100.0))

    assert(target == result)
  }

  test("Criteria 29. Computing Average Value Per Property should match") {

    val prefix = "http://ex.com/bar#"

    val path: String = this.getClass.getClassLoader.getResource("max_per_numericDatatype.owl").getPath

    val axioms = spark.owl(syntax)(path)

    val criteria = new OWLStats(spark).getAvgPerNumericDatatypeProperty(axioms)

    val result = criteria.collect().toSet

    val target = Set((df.getOWLDataProperty(prefix + "dataProp1"), (20 + 2320 + (-400)) / 3.0),
                     (df.getOWLDataProperty(prefix + "dataProp2"), (70 + (-25) + 11) / 3.0),
                     (df.getOWLDataProperty(prefix + "dataProp3"), (10.0 + 123.0 + (-895.0)) / 3),
                     (df.getOWLDataProperty(prefix + "dataProp4"), (50.0 + (-20.0) + 100.0) / 3))

    assert(target == result)
  }

  test("Criteria 30. Computing Subject Vocabularies should result in size 2") {

    val criteria = new OWLStats(spark).getSubjectVocabularies(rdd)
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("Criteria 31. Computing Predicate Vocabularies should result in size 2") {

    val criteria = new OWLStats(spark).getPredicateVocabularies(rdd)
    val cnt = criteria.count()

    assert(cnt == 2)
  }

  test("Criteria 32. Computing Object Vocabularies should result in size 4") {

    val criteria = new OWLStats(spark).getObjectVocabularies(rdd)
    val cnt = criteria.count()

    assert(cnt == 4)
  }


}
