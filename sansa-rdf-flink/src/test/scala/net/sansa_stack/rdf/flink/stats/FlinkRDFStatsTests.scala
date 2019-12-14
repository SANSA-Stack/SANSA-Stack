package net.sansa_stack.rdf.flink.stats

import net.sansa_stack.rdf.flink.io._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class FlinkRDFStatsTests extends FunSuite {

  import net.sansa_stack.rdf.flink.stats._

  val env = ExecutionEnvironment.getExecutionEnvironment

  test("1. computing used classes should match") {
    val path = getClass.getResource("/data.nt").getPath

    val triples = env.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsUsedClasses()

    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing distinct subjects should match") {
    val path = getClass.getResource("/data.nt").getPath

    val triples = env.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsDistinctSubjects()

    val cnt = criteria.count()

    assert(cnt == 106)
  }

  test("computing Class Usage Count should match") {
    val path = getClass.getResource("/data.nt").getPath

    val triples = env.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsClassUsageCount()

    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Classes Defined should match") {
    val path = getClass.getResource("/data.nt").getPath

    val triples = env.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsClassesDefined()

    val cnt = criteria.count()

    assert(cnt == 0)
  }

  test("computing Property Usage should match") {
    val path = getClass.getResource("/data.nt").getPath

    val triples = env.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsPropertyUsage()

    val cnt = criteria.count()

    assert(cnt == 24)
  }

  test("computing Distinct Entities should match") {
    val path = getClass.getResource("/data.nt").getPath

    val triples = env.rdf(Lang.NTRIPLES)(path)

    val criteria = triples.statsDistinctEntities()

    criteria.collect().foreach(println(_))

    val cnt = criteria.count()

    assert(cnt == 26)
  }

}
