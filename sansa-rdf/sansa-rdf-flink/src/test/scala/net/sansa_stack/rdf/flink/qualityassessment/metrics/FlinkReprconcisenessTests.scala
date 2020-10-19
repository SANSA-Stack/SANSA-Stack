package net.sansa_stack.rdf.flink.qualityassessment.metrics

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.scalatest.FunSuite

class FlinkReprconcisenessTests extends FunSuite {

  import net.sansa_stack.rdf.flink.qualityassessment._

  val conf = new Configuration()
  conf.setDouble("taskmanager.network.memory.fraction", 0.4)
  conf.setString("taskmanager.network.memory.max", "2GB")
  val env = ExecutionEnvironment.createLocalEnvironment(conf)
  env.setParallelism(4)

  test("assessing the query param free URIs should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val value = triples.assessQueryParamFreeURIs()
    assert(value == 0.0)
  }

  test("assessing the short URIs should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val value = triples.assessShortURIs()
    assert(value == 0.0)
  }

}
