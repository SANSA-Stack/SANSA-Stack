package net.sansa_stack.rdf.flink.qualityassessment.metrics

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.funsuite.AnyFunSuite

class FlinkRelevancyTests extends AnyFunSuite {

  import net.sansa_stack.rdf.flink.qualityassessment._

  val env = ExecutionEnvironment.getExecutionEnvironment

  test("assessing the amount of triples should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val cnt = triples.assessAmountOfTriples()
    assert(cnt == 0.0)
  }

  test("assessing the coverage scope of a dataset should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val ratio = triples.assessCoverageScope()
    assert(ratio == 0.0)
  }

  test("assessing the coverage details of a dataset should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val ratio = triples.assessCoverageDetail()
    assert(ratio == 0.22641509433962265)
  }

}
