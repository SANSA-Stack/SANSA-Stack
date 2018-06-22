package net.sansa_stack.examples.spark.query

import net.sansa_stack.query.spark.graph.jena.SparqlParser
import net.sansa_stack.query.spark.graph.jena.model.{IntermediateResult, SparkExecutionModel, Config => modelConfig}
import org.apache.jena.riot.Lang
import org.apache.log4j.Logger

import scala.concurrent.duration._

object graph {

  def main(args: Array[String]): Unit = {

    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None =>
    }
  }

  def run(config: Config): Unit = {

    println("===========================================")
    println("| SANSA - Graph query example     |")
    println("===========================================")

    val log = Logger.getLogger(graph.getClass)

    // set configures for query engine model
    modelConfig.setAppName("SANSA Graph Query")
      .setInputGraphFile(config.input)
      .setInputQueryFile(config.query.head)
      .setLang(Lang.NTRIPLES)
      .setMaster("local[*]")

    // load graph
    log.info("Start to load graph")

    SparkExecutionModel.createSparkSession()

    // apply graph partitioning algorithm
    config.algo match {
      case "SSHP" => //compiler here
      case "OSHP" => //compiler here
      case "SOSHP" => //compiler here
      case "PP" => //compiler here
      case "" =>
      case other => println("the input of algorithm doesn't any options, no algorithm will be applied.")
    }

    // query executing
    log.info("Start to execute queries")

    var start = 0L
    var end = 0L

    config.query.foreach{ path =>
      log.info("Query file: "+path)
      modelConfig.setInputQueryFile(path)
      val sp = new SparqlParser(modelConfig.getInputQueryFile)
      sp.getOps.foreach{ ops =>
        val tag = ops.getTag
        log.info("Operation "+tag+" start")
        start = System.currentTimeMillis()
        ops.execute()
        end = System.currentTimeMillis()
        log.info(tag+" execution time: "+Duration(end - start, "millis").toMillis+" ms")
      }
    }

    // print results to console
    if(config.print){
      log.info("print final result")
      val results = IntermediateResult.getFinalResult.cache()
      if(results.count() >= 10){
        log.info("Too long results(more than 10)")
      } else {
        results.foreach(println(_))
      }
      results.unpersist()
    }
  }

  case class Config(input: String = "", query: Seq[String] = null, print: Boolean = false, algo: String = "")

  val parser: scopt.OptionParser[Config] = new scopt.OptionParser[Config]("Spark-Graph-Example") {

    head("SANSA-Query-Graph-Example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(input = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[Seq[String]]('q', "query").required().valueName("<query1>, <query2>...").
      action((x, c) => c.copy(query = x)).
      text("files that contain the SPARQL query")

    opt[Boolean]('p', "print").optional().valueName("Boolean").
      action((_, c) => c.copy(print = true)).
      text("print the result to the console, default: false")

    opt[String]('a', "algorithm").optional().valueName("SSHP | OSHP | SOSHP | PP").
      action((x, c) => c.copy(algo = x)).
      text("choose one graph partitioning algorithm (default no algorithm applied)")

    help("help").text("prints this usage text")
  }
}
