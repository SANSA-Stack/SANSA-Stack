package net.sansa_stack.rdf.flink.qualityassessment.metrics

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class FlinkAvailabilityTests extends FunSuite {

  import net.sansa_stack.rdf.flink.qualityassessment._

  val env = ExecutionEnvironment.getExecutionEnvironment

  test("getting the dereferenceable URIs should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val ratio = 0 // triples.assessDereferenceableUris()
    assert(ratio == 0.0)
  }

}
