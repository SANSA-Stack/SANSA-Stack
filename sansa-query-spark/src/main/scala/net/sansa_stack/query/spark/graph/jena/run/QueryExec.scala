package net.sansa_stack.query.spark.graph.jena.run

import net.sansa_stack.query.spark.graph.jena.SparqlParser
import net.sansa_stack.query.spark.graph.jena.model.{Config, IntermediateResult, SparkExecutionModel}
import org.apache.jena.riot.Lang
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.concurrent.duration._

object QueryExec {
  def main(args: Array[String]): Unit = {

    val master = "local[*]"
    val graphPath = "sansa-query-spark/src/test/resources/benchmark_50k.nt"
    val queryPath = "sansa-query-spark/src/test/resources/BSBM/query_1.txt"

    val log = Logger.getLogger(QueryExec.getClass)

    Config.setMaster(master)
      .setAppName("SPARQL for GRAPHX")
      .setInputGraphFile(graphPath)
      .setInputQueryFile(queryPath)
      .setLang(Lang.NTRIPLES)

    // load graph
    log.info("Start to load graph")
    val loadStart = System.currentTimeMillis()
    SparkExecutionModel.loadGraph()
    val loadEnd = System.currentTimeMillis()
    val loadDuration = Duration(loadEnd - loadStart, "millis")
    log.info("Loading time: "+loadDuration)

    val sp = new SparqlParser(Config.getInputQueryFile)

    // query executing
    log.info("Start to execute query")
    val queryStart = System.currentTimeMillis()
    sp.getOps.foreach { ops =>
      val tag = ops.getTag
      log.info("Operation "+tag+" start")
      val opStart = System.currentTimeMillis()
      ops.execute()
      val opEnd = System.currentTimeMillis()
      val duration = Duration(opEnd - opStart, "millis")
      log.info("Operation "+tag+" end. Execute Time: "+duration)
    }
    val queryEnd = System.currentTimeMillis()
    val queryDuration = Duration(queryEnd - queryStart, "millis")
    log.info("Loading time: "+queryDuration)
  }
}
