package net.sansa_stack.owl.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import net.sansa_stack.owl.spark.owl._
import org.apache.spark.sql.SparkSession


class ManchesterSyntaxOWLExpressionsRDDBuilderTest extends FunSuite with SharedSparkContext {
  lazy val spark = SparkSession.builder().appName(sc.appName).master(sc.master)
    .config(
      "spark.kryo.registrator",
      "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
    .getOrCreate()
    
  var _rdd: OWLExpressionsRDD = null
  val syntax = Syntax.MANCHESTER

  def rdd = {
    if (_rdd == null) {
      _rdd = spark.owlExpressions(syntax)("src/test/resources/ont_manchester.owl")
//        sc, "hdfs://localhost:9000/ont_manchester.owl")
      _rdd.cache()
    }

    _rdd
  }

  test("The total number of frames should be correct") {
    val total = 50
    assert(rdd.count() == total)
  }

  test("The number of Class frames should be correct") {
    val expectedNumClassFrames = 21
    val actualNumClassFrames = rdd.filter(_.trim.startsWith("Class:")).count()
    assert(actualNumClassFrames == expectedNumClassFrames)
  }

  test("The number of AnnotationProperty frames should be correct") {
    val expectedNumAnnoPropertyFrames = 8
    val actualNumAnnoPropertyFrames =
      rdd.filter(_.trim.startsWith("AnnotationProperty:")).count()

    assert(actualNumAnnoPropertyFrames == expectedNumAnnoPropertyFrames)
  }

  test("The number of ObjectProperty frames should be correct") {
    val expectedNumObjPropertyFrames = 7
    val actualNumObjPropertyFrames =
      rdd.filter(_.trim.startsWith("ObjectProperty:")).count()

    assert(actualNumObjPropertyFrames == expectedNumObjPropertyFrames)
  }

  test("The number of DataProperty frames should be correct") {
    val expectedNumDataPropertyFrames = 4
    val actualNumDataPropertyFrames =
      rdd.filter(_.trim.startsWith("DataProperty:")).count()

    assert(actualNumDataPropertyFrames == expectedNumDataPropertyFrames)
  }

  test("The number of Individual frames should be correct") {
    val expectedNumIndividualFrames = 3
    val actualNumIndividualFrames =
      rdd.filter(_.trim.startsWith("Individual:")).count()

    assert(actualNumIndividualFrames == expectedNumIndividualFrames)
  }

  test("The number of DisjointClasses frames should be correct") {
    val expectedNumDisjointClassesFrames = 0
    val actualNumDisjointClassesFrames =
      rdd.filter(_.trim.startsWith("DisjointClasses:")).count()

    assert(actualNumDisjointClassesFrames == expectedNumDisjointClassesFrames)
  }

  test("The number of EquivalentClasses frames should be correct") {
    val expectedNumEquivalentClassesFrames = 0
    val actualNumEquivalentClassesFrames =
      rdd.filter(_.trim.startsWith("EquivalentClasses:")).count()

    assert(actualNumEquivalentClassesFrames == expectedNumEquivalentClassesFrames)
  }

  test("The number of EquivalentProperties frames should be correct") {
    val expectedNumEquivalentProperties = 0
    val actualNumEquivalentProperties =
      rdd.filter(_.trim.startsWith("EquivalentProperties:")).count()

    assert(actualNumEquivalentProperties == expectedNumEquivalentProperties)
  }

  test("The number of DisjointProperties frames should be correct") {
    val expectedNumDisjointProperties = 0
    val actualNumDisjointProperties =
      rdd.filter(_.trim.startsWith("DisjointProperties:")).count()

    assert(actualNumDisjointProperties == expectedNumDisjointProperties)
  }

  test("The number of SameIndividual frames should be correct") {
    val expectedNumSameIndividualFrames = 0
    val actualNumSameIndividualFrames =
      rdd.filter(_.trim.startsWith("SameIndividual:")).count()

    assert(actualNumSameIndividualFrames == expectedNumSameIndividualFrames)
  }

  test("The number of DifferentIndividuals frames should be correct") {
    val expectedNumDifferentIndividualsFrames = 0
    val actualNumDifferentIndividualsFrame =
      rdd.filter(_.trim.startsWith("DifferentIndividuals:")).count()

    assert(actualNumDifferentIndividualsFrame == expectedNumDifferentIndividualsFrames)
  }

  test("The number of Datatype frames should be correct") {
    val expectedNumDatatypeFrames = 7
    val actualNumDatatypeFrames =
      rdd.filter(_.trim.startsWith("Datatype:")).count()

    assert(actualNumDatatypeFrames == expectedNumDatatypeFrames)
  }
}
