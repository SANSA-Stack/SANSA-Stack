package net.sansa_stack.inference.spark

import java.io.File

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.rules.ReasoningProfile._
import net.sansa_stack.inference.rules.{RDFSLevel, ReasoningProfile}
import net.sansa_stack.inference.spark.data.{RDFGraphLoader, RDFGraphWriter}
import net.sansa_stack.inference.spark.forwardchaining.{ForwardRuleReasonerOWLHorst, ForwardRuleReasonerRDFS}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * The main entry class to compute the materialization on an RDF graph.
  * Currently, only RDFS and OWL-Horst are supported.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphMaterializer {


  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out, config.profile, config.writeToSingleFile, config.sortedOutput)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: Seq[File], output: File, profile: ReasoningProfile, writeToSingleFile: Boolean, sortedOutput: Boolean): Unit = {
    val conf = new SparkConf()
    conf.registerKryoClasses(Array(classOf[RDFTriple]))

    // the SPARK config
    val session = SparkSession.builder
      .appName(s"SPARK $profile Reasoning")
      .master("local[4]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") //override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", "4")
      .config(conf)
      .getOrCreate()

    // load triples from disk
    val graph = RDFGraphLoader.loadFromDisk(input, session.sparkContext, 4)
    println(s"|G| = ${graph.size()}")

    // create reasoner
    val reasoner = profile match {
      case RDFS => new ForwardRuleReasonerRDFS(session.sparkContext)
      case RDFS_SIMPLE => {
        val r = new ForwardRuleReasonerRDFS(session.sparkContext)
        r.level = RDFSLevel.SIMPLE
        r
      }
      case OWL_HORST => new ForwardRuleReasonerOWLHorst(session.sparkContext)
    }

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)
    println(s"|G_inf| = ${inferredGraph.size()}")

    // write triples to disk
    RDFGraphWriter.writeGraphToFile(inferredGraph, output.getAbsolutePath, writeToSingleFile, sortedOutput)

    session.stop()
  }

  // the config object
  case class Config(
                     in: Seq[File] = Seq(),
                     out: File = new File("."),
                     profile: ReasoningProfile = ReasoningProfile.RDFS,
                     writeToSingleFile: Boolean = false,
                     sortedOutput: Boolean = false)

  // read ReasoningProfile enum
  implicit val profilesRead: scopt.Read[ReasoningProfile.Value] =
    scopt.Read.reads(ReasoningProfile forName _.toLowerCase())

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("RDFGraphMaterializer") {
    head("RDFGraphMaterializer", "0.1.0")

    opt[Seq[File]]('i', "input").required().valueName("<path1>,<path2>,...").
      action((x, c) => c.copy(in = x)).
      text("path to file or directory that contains the input files (in N-Triple format)")

    opt[File]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    opt[Unit]("single-file").optional().action( (_, c) =>
      c.copy(writeToSingleFile = true)).text("write the output to a single file in the output directory")

    opt[Unit]("sorted").optional().action( (_, c) =>
      c.copy(sortedOutput = true)).text("sorted output of the triples (per file)")

    opt[ReasoningProfile]('p', "profile").required().valueName("{rdfs | rdfs-simple | owl-horst}").
      action((x, c) => c.copy(profile = x)).
      text("the reasoning profile")

    help("help").text("prints this usage text")
  }
}
