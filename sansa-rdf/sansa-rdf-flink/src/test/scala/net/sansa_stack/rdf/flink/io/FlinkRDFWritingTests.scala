package net.sansa_stack.rdf.flink.io

import java.nio.file.Files

import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.funsuite.AnyFunSuite

class FlinkRDFWritingTests extends AnyFunSuite {

  val env = ExecutionEnvironment.getExecutionEnvironment

  test("writing N-Triples file from DataSet to disk should match") {

    val path = getClass.getResource("/data.nt").getPath
    val lang = Lang.NTRIPLES

    val triples = env.rdf(lang)(path)

    val cnt = triples.count()

    assert(cnt == 106)

    // create temp dir
    val outputDir = Files.createTempDirectory("sansa-graph")
    outputDir.toFile.deleteOnExit()

    triples.saveAsNTriplesFile(outputDir.toString())

    // load again
    val triples2 = env.rdf(lang)(path)

    // and check if count is the same
    val cnt2 = triples2.count()
    assert(cnt2 == cnt)
  }

}
