package net.sansa_stack.rdf.flink.qualityassessment.metrics

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.funsuite.AnyFunSuite

class FlinkCompletenessTests extends AnyFunSuite {

  import net.sansa_stack.rdf.flink.qualityassessment._

  val env = ExecutionEnvironment.getExecutionEnvironment

  test("assessing the Interlinking Completeness should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val value = triples.assessInterlinkingCompleteness()
    assert(value == 0)
  }

  test("assessing the Property Completeness should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val value = triples.assessPropertyCompleteness()
    assert(value == 0)
  }

  test("assessing the Schema Completeness should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val ratio = triples.assessSchemaCompleteness()
    assert(ratio == 0.0)
  }


}
