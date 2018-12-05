package net.sansa_stack.rdf.flink.stats

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class FlinkRDFStatsTests extends FunSuite {
  val env = ExecutionEnvironment.getExecutionEnvironment

  import net.sansa_stack.rdf.flink.stats._

  test("computing used classes should result in size 0") {
    val input = "src/test/resources/rdf.nt"

    val triples = env.rdf(Lang.NTRIPLES)(input)

    val criteria = triples.statsUsedClasses()
    val cnt = criteria.count()

    assert(cnt == 0)
  }
}
