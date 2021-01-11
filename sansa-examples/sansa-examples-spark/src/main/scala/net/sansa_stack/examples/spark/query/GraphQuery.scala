package net.sansa_stack.examples.spark.query

import net.sansa_stack.query.spark.graph.jena.SparqlParser
import net.sansa_stack.query.spark.graph.jena.model.{IntermediateResult, SparkExecutionModel, Config => modelConfig}
import net.sansa_stack.rdf.spark.partition.graph.algo._
import org.apache.jena.graph.Node
import org.apache.jena.riot.Lang
import org.apache.log4j.Logger
import org.apache.spark.graphx.Graph

import scala.concurrent.duration.Duration

object GraphQuery {

  def main(args: Array[String]): Unit = {

    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None =>
        println(parser.usage)
    }
  }

  def run(config: Config): Unit = {

    println("===========================================")
    println("| SANSA - Graph query example     |")
    println("===========================================")

    val log = Logger.getLogger(GraphQuery.getClass)

    // set configures for query engine model
    modelConfig.setAppName("SANSA Graph Query")
      .setInputGraphFile(config.input)
      .setInputQueryFile(config.query.head)
      .setLang(Lang.NTRIPLES)

    // load graph
    log.info("Start to load graph")

    SparkExecutionModel.createSparkSession()
    val session = SparkExecutionModel.getSession

    // apply graph partitioning algorithm
    val prevG = SparkExecutionModel.getGraph
    var g: Graph[Node, Node] = null
    var msg: String = null
    var numParts: Int = 0
    var numIters: Int = 0

    // Set number of partitions (if config.numParts is 0, number of partitions equals to that of previous graph)
    config.numParts match {
      case 0 => numParts = prevG.edges.partitions.length
      case other => numParts = other
    }

    config.numIters match {
      case 0 =>
      case other => numIters = other
    }

    var partAlgo: PartitionAlgo[Node, Node] = null

    config.algo match {
      case "SSHP" =>
        if (numIters == 0) {
          // Partition algorithm will use default number of iterations
          partAlgo = new SubjectHashPartition[Node, Node](prevG, session, numParts)
        } else {
          partAlgo = new SubjectHashPartition[Node, Node](prevG, session, numParts).setNumIterations(numIters)
        }
        msg = "Start to execute subject semantic hash partitioning"
      case "OSHP" =>
        if (numIters == 0) {
          partAlgo = new ObjectHashPartition[Node, Node](prevG, session, numParts)
        } else {
          partAlgo = new ObjectHashPartition[Node, Node](prevG, session, numParts).setNumIterations(numIters)
        }
        msg = "Start to execute object semantic hash partitioning"
      case "SOSHP" =>
        if (numIters == 0) {
          partAlgo = new SOHashPartition[Node, Node](prevG, session, numParts)
        } else {
          partAlgo = new SOHashPartition[Node, Node](prevG, session, numParts).setNumIterations(numIters)
        }
        msg = "Start to execute subject-object semantic hash partitioning"
      case "PP" =>
        if (numIters == 0) {
          partAlgo = new PathPartition[Node, Node](prevG, session, numParts)
        } else {
          partAlgo = new PathPartition[Node, Node](prevG, session, numParts).setNumIterations(numIters)
        }
        msg = "Start to execute path partitioning"
      case "" =>
      case other => println(s"the input $other doesn't match any options, no algorithm will be applied.")
    }

    var start = 0L
    var end = 0L

    if (partAlgo != null) {
      log.info(msg)
      start = System.currentTimeMillis()
      g = partAlgo.partitionBy().cache()
      SparkExecutionModel.loadGraph(g)
      end = System.currentTimeMillis()
      log.info("Graph partitioning execution time: " + Duration(end - start, "millis").toMillis + " ms")
    }

    // query executing
    log.info("Start to execute queries")

    config.query.foreach { path =>
      log.info("Query file: " + path)
      modelConfig.setInputQueryFile(path)
      val sp = new SparqlParser(modelConfig.getInputQueryFile)
      sp.getOps.foreach { ops =>
        val tag = ops.getTag
        log.info("Operation " + tag + " start")
        start = System.currentTimeMillis()
        ops.execute()
        end = System.currentTimeMillis()
        log.info(tag + " execution time: " + Duration(end - start, "millis").toMillis + " ms")
      }
    }

    // print results to console
    if (config.print) {
      log.info("print final result")
      val results = IntermediateResult.getFinalResult.cache()
      if (results.count() >= 10) {
        log.info("Too long results(more than 10)")
      } else {
        results.collect().foreach(println(_))
      }
      results.unpersist()
    }
  }

  case class Config(input: String = "", query: Seq[String] = null, print: Boolean = false, algo: String = "",
                    numParts: Int = 0, numIters: Int = 0)

  val parser: scopt.OptionParser[Config] = new scopt.OptionParser[Config]("Spark-Graph-Example") {

    head("SANSA-Query-Graph-Example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(input = x)).
      text("path to file that contains the data (in N-Triples format).")

    opt[Seq[String]]('q', "query").required().valueName("<query1>, <query2>...").
      action((x, c) => c.copy(query = x)).
      text("files that contain SPARQL queries.")

    opt[Boolean]('p', "print").optional().valueName("Boolean").
      action((_, c) => c.copy(print = true)).
      text("print the result to the console(maximum 10 rows), default: false.")

    opt[String]('a', "algorithm").optional().valueName("<SSHP | OSHP | SOSHP | PP>").
      action((x, c) => c.copy(algo = x)).
      text("choose one graph partitioning algorithm, default: no algorithm applied.")

    opt[Int]('n', "number of partitions").optional().valueName("<Int>")
      .action((x, c) => c.copy(numParts = x))
      .text("set the number of partitions.")

    opt[Int]('t', "number of iterations").optional().valueName("<Int>")
      .action((x, c) => c.copy(numIters = x))
      .text("set the number of iterations.")

    help("help").text("prints this usage text")
  }
}
