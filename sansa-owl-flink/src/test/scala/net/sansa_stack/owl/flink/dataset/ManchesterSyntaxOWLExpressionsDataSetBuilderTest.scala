package net.sansa_stack.owl.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.FunSuite


class ManchesterSyntaxOWLExpressionsDataSetBuilderTest extends FunSuite {
  lazy val env = ExecutionEnvironment.getExecutionEnvironment
  var _dataSet: OWLExpressionsDataSet = null
  def dataSet: OWLExpressionsDataSet = {
    if (_dataSet == null) {
      _dataSet = ManchesterSyntaxOWLExpressionsDataSetBuilder.build(
        env, this.getClass.getClassLoader.getResource("ont_manchester.owl").getPath)
//        env, "hdfs://localhost:9000/ont_manchester.owl")
    }
    _dataSet
  }

  test("The total number of frames should be correct") {
    val total = 49
    assert(dataSet.count() == total)
  }

  test("The number of Class frames should be correct") {
    val expectedNumClassFrames = 21
    val actualNumClassFrames = dataSet.filter(_.trim.startsWith("Class:")).count()
    assert(actualNumClassFrames == expectedNumClassFrames)
  }

  test("The number of AnnotationProperty frames should be correct") {
    val expectedNumAnnoPropertyFrames = 7
    val actualNumAnnoPropertyFrames =
      dataSet.filter(_.trim.startsWith("AnnotationProperty:")).count()

    assert(actualNumAnnoPropertyFrames == expectedNumAnnoPropertyFrames)
  }

  test("The number of ObjectProperty frames should be correct") {
    val expectedNumObjPropertyFrames = 7
    val actualNumObjPropertyFrames =
      dataSet.filter(_.trim.startsWith("ObjectProperty:")).count()

    assert(actualNumObjPropertyFrames == expectedNumObjPropertyFrames)
  }

  test("The number of DataProperty frames should be correct") {
    val expectedNumDataPropertyFrames = 4
    val actualNumDataPropertyFrames =
      dataSet.filter(_.trim.startsWith("DataProperty:")).count()

    assert(actualNumDataPropertyFrames == expectedNumDataPropertyFrames)
  }

  test("The number of Individual frames should be correct") {
    val expectedNumIndividualFrames = 3
    val actualNumIndividualFrames =
      dataSet.filter(_.trim.startsWith("Individual:")).count()

    assert(actualNumIndividualFrames == expectedNumIndividualFrames)
  }

  test("The number of DisjointClasses frames should be correct") {
    val expectedNumDisjointClassesFrames = 0
    val actualNumDisjointClassesFrames =
      dataSet.filter(_.trim.startsWith("DisjointClasses:")).count()

    assert(actualNumDisjointClassesFrames == expectedNumDisjointClassesFrames)
  }

  test("The number of EquivalentClasses frames should be correct") {
    val expectedNumEquivalentClassesFrames = 0
    val actualNumEquivalentClassesFrames =
      dataSet.filter(_.trim.startsWith("EquivalentClasses:")).count()

    assert(actualNumEquivalentClassesFrames == expectedNumEquivalentClassesFrames)
  }

  test("The number of EquivalentProperties frames should be correct") {
    val expectedNumEquivalentProperties = 0
    val actualNumEquivalentProperties =
      dataSet.filter(_.trim.startsWith("EquivalentProperties:")).count()

    assert(actualNumEquivalentProperties == expectedNumEquivalentProperties)
  }

  test("The number of DisjointProperties frames should be correct") {
    val expectedNumDisjointProperties = 0
    val actualNumDisjointProperties =
      dataSet.filter(_.trim.startsWith("DisjointProperties:")).count()

    assert(actualNumDisjointProperties == expectedNumDisjointProperties)
  }

  test("The number of SameIndividual frames should be correct") {
    val expectedNumSameIndividualFrames = 0
    val actualNumSameIndividualFrames =
      dataSet.filter(_.trim.startsWith("SameIndividual:")).count()

    assert(actualNumSameIndividualFrames == expectedNumSameIndividualFrames)
  }

  test("The number of DifferentIndividuals frames should be correct") {
    val expectedNumDifferentIndividualsFrames = 0
    val actualNumDifferentIndividualsFrame =
      dataSet.filter(_.trim.startsWith("DifferentIndividuals:")).count()

    assert(actualNumDifferentIndividualsFrame == expectedNumDifferentIndividualsFrames)
  }

  test("The number of Datatype frames should be correct") {
    val expectedNumDatatypeFrames = 7
    val actualNumDatatypeFrames =
      dataSet.filter(_.trim.startsWith("Datatype:")).count()

    assert(actualNumDatatypeFrames == expectedNumDatatypeFrames)
  }
}
