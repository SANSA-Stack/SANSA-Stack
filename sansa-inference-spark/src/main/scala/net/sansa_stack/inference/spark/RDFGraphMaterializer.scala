package net.sansa_stack.inference.spark

import java.io.File

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.data.{RDFGraphLoader, RDFGraphWriter}
import net.sansa_stack.inference.spark.forwardchaining.ForwardRuleReasonerRDFS
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * The class to compute the RDFS materialization of a given RDF graph.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphMaterializer {


  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: File, output: File) = {
    val conf = new SparkConf()
    conf.registerKryoClasses(Array(classOf[RDFTriple]))

    // the SPARK config
    val session = SparkSession.builder
      .appName("SPARK Reasoning")
      .master("local[4]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") //override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config(conf)
      .getOrCreate()

    // load triples from disk
    val graph = RDFGraphLoader.loadFromFile(input.getAbsolutePath, session.sparkContext, 4)

    // create reasoner
    val reasoner = new ForwardRuleReasonerRDFS(session.sparkContext)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)
    print(inferredGraph.size())

    // write triples to disk
    RDFGraphWriter.writeToFile(inferredGraph, output.getAbsolutePath)

    session.stop()
  }

  case class Config(in: File = new File("."), out: File = new File("."))

  val parser = new scopt.OptionParser[Config]("RDFGraphMaterializer") {
    head("RDFGraphMaterializer", "0.1.0")

    opt[File]('i', "input").required().valueName("<file>").
      action((x, c) => c.copy(in = x)).
      text("the input file in N-Triple format")

    opt[File]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    help("help").text("prints this usage text")
  }

}
