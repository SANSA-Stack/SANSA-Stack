package net.sansa_stack.rdf.flink.model

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.funsuite.AnyFunSuite

class FlinkGraphTripleOpsTests extends AnyFunSuite {

  val env = ExecutionEnvironment.getExecutionEnvironment
  test("constructing the Graph from DataSet should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES


    val triples = env.rdf(lang)(path)

    // val graph = triples.asGraph()

    // val size = graph.size()

    assert(triples.count() == 106)

  }
}
