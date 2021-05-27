package net.sansa_stack.owl.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.FunSuite

class ManchesterSyntaxOWLExpressionsDataSetBuilderTest extends FunSuite {
  import net.sansa_stack.owl.flink.owl._
  lazy val env = ExecutionEnvironment.getExecutionEnvironment
  var _dataSet: OWLExpressionsDataSet = null
  val syntax = Syntax.MANCHESTER

  def dataSet: OWLExpressionsDataSet = {
    if (_dataSet == null) {
      _dataSet = env.owlExpressions(syntax)(this.getClass.getClassLoader.getResource("ont_manchester.owl").getPath)
    }
    _dataSet
  }

  ignore("The total number of frames should be correct") {
    val total = 49
    assert(dataSet.count() == total)
  }

  ignore("The number of Class frames should be correct") {
    val expectedNumClassFrames = 21
    val actualNumClassFrames = dataSet.filter(_.trim.startsWith("Class:")).count()
    assert(actualNumClassFrames == expectedNumClassFrames)
  }

  ignore("The number of AnnotationProperty frames should be correct") {
    val expectedNumAnnoPropertyFrames = 7
    val actualNumAnnoPropertyFrames =
      dataSet.filter(_.trim.startsWith("AnnotationProperty:")).count()

    assert(actualNumAnnoPropertyFrames == expectedNumAnnoPropertyFrames)
  }

  ignore("The number of ObjectProperty frames should be correct") {
    val expectedNumObjPropertyFrames = 7
    val actualNumObjPropertyFrames =
      dataSet.filter(_.trim.startsWith("ObjectProperty:")).count()

    assert(actualNumObjPropertyFrames == expectedNumObjPropertyFrames)
  }

  ignore("The number of DataProperty frames should be correct") {
    val expectedNumDataPropertyFrames = 4
    val actualNumDataPropertyFrames =
      dataSet.filter(_.trim.startsWith("DataProperty:")).count()

    assert(actualNumDataPropertyFrames == expectedNumDataPropertyFrames)
  }

  ignore("The number of Individual frames should be correct") {
    val expectedNumIndividualFrames = 3
    val actualNumIndividualFrames =
      dataSet.filter(_.trim.startsWith("Individual:")).count()

    assert(actualNumIndividualFrames == expectedNumIndividualFrames)
  }

  ignore("The number of DisjointClasses frames should be correct") {
    val expectedNumDisjointClassesFrames = 0
    val actualNumDisjointClassesFrames =
      dataSet.filter(_.trim.startsWith("DisjointClasses:")).count()

    assert(actualNumDisjointClassesFrames == expectedNumDisjointClassesFrames)
  }

  ignore("The number of EquivalentClasses frames should be correct") {
    val expectedNumEquivalentClassesFrames = 0
    val actualNumEquivalentClassesFrames =
      dataSet.filter(_.trim.startsWith("EquivalentClasses:")).count()

    assert(actualNumEquivalentClassesFrames == expectedNumEquivalentClassesFrames)
  }

  ignore("The number of EquivalentProperties frames should be correct") {
    val expectedNumEquivalentProperties = 0
    val actualNumEquivalentProperties =
      dataSet.filter(_.trim.startsWith("EquivalentProperties:")).count()

    assert(actualNumEquivalentProperties == expectedNumEquivalentProperties)
  }

  ignore("The number of DisjointProperties frames should be correct") {
    val expectedNumDisjointProperties = 0
    val actualNumDisjointProperties =
      dataSet.filter(_.trim.startsWith("DisjointProperties:")).count()

    assert(actualNumDisjointProperties == expectedNumDisjointProperties)
  }

  ignore("The number of SameIndividual frames should be correct") {
    val expectedNumSameIndividualFrames = 0
    val actualNumSameIndividualFrames =
      dataSet.filter(_.trim.startsWith("SameIndividual:")).count()

    assert(actualNumSameIndividualFrames == expectedNumSameIndividualFrames)
  }

  ignore("The number of DifferentIndividuals frames should be correct") {
    val expectedNumDifferentIndividualsFrames = 0
    val actualNumDifferentIndividualsFrame =
      dataSet.filter(_.trim.startsWith("DifferentIndividuals:")).count()

    assert(actualNumDifferentIndividualsFrame == expectedNumDifferentIndividualsFrames)
  }

  ignore("The number of Datatype frames should be correct") {
    val expectedNumDatatypeFrames = 7
    val actualNumDatatypeFrames =
      dataSet.filter(_.trim.startsWith("Datatype:")).count()

    assert(actualNumDatatypeFrames == expectedNumDatatypeFrames)
  }
}
