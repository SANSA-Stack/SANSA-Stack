package net.sansa_stack.rdf.flink.qualityassessment.metrics

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.funsuite.AnyFunSuite

class FlinkSyntacticvalidityTests extends AnyFunSuite {

  import net.sansa_stack.rdf.flink.qualityassessment._

  val env = ExecutionEnvironment.getExecutionEnvironment

  test("assessing the literal numeric range checker should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val value = triples.assessLiteralNumericRangeChecker()
    assert(value == 0.0)
  }

  test("assessing the XSD datatype compatible literals should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val value = triples.assessXSDDatatypeCompatibleLiterals()
    assert(value == 47)
  }

}
