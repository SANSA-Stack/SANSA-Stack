package net.sansa_stack.rdf.flink.qualityassessment.metrics

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.funsuite.AnyFunSuite

class FlinkAvailabilityTests extends AnyFunSuite {

  import net.sansa_stack.rdf.flink.qualityassessment._

  val env = ExecutionEnvironment.createLocalEnvironment(4)

  test("getting the Dereferenceable URIs should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val ratio = triples.assessDereferenceableUris()
    assert(ratio == 0.1320754716981132)
  }

  test("getting the Dereferenceable BackLinks should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val ratio = triples.assessDereferenceableBackLinks()
    assert(ratio == 0.15384615384615385)
  }

  test("getting the Dereferenceable ForwardLinks should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val ratio = triples.assessDereferenceableForwardLinks()
    assert(ratio == 0.05660377358490566)
  }

}
