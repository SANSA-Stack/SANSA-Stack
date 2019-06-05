package net.sansa_stack.rdf.flink.stats

import net.sansa_stack.rdf.flink.io._
import org.apache.jena.riot.Lang
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest.FunSuite

class FlinkRDFStatsTests extends FunSuite {

  import net.sansa_stack.rdf.flink.stats._

  val env = ExecutionEnvironment.getExecutionEnvironment

  test("1. computing used classes should result in size 0") {
    val path = getClass.getResource("/data.nt").getPath

    val triples = env.rdf(Lang.NTRIPLES)(path)
    
    val criteria = triples.statsUsedClasses()
    
    val cnt = criteria.count()

    assert(cnt == 0)
  }

}
