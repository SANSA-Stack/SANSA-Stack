package net.sansa_stack.inference.spark

import java.io.File
import java.net.URI
import java.nio.file.{Path, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.rules.ReasoningProfile._
import net.sansa_stack.inference.rules.{RDFSLevel, ReasoningProfile}
import net.sansa_stack.inference.spark.data.{RDFGraphLoader, RDFGraphWriter}
import net.sansa_stack.inference.spark.forwardchaining.{ForwardRuleReasonerOWLHorst, ForwardRuleReasonerRDFS, ForwardRuleReasonerRDFSDataset, TransitiveReasoner}

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
        run(config.in, config.out, config.profile, config.properties, config.writeToSingleFile, config.sortedOutput)
      case None =>
        // scalastyle:off println
        println(parser.usage)
        // scalastyle:on println
    }
  }

  def run(input: Seq[URI], output: File, profile: ReasoningProfile, properties: Seq[String] = Seq(),
          writeToSingleFile: Boolean, sortedOutput: Boolean): Unit = {
    // register the custom classes for Kryo serializer
    val conf = new SparkConf()
    conf.registerKryoClasses(Array(classOf[RDFTriple]))
    conf.set("spark.extraListeners", "net.sansa_stack.inference.spark.utils.CustomSparkListener")

    // set the parallelism
    val parallelism = 4

    // the SPARK config
    val session = SparkSession.builder
      .appName(s"SPARK $profile Reasoning")
      .master("local[4]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", parallelism)
      .config("spark.ui.showConsoleProgress", "false")
      .config(conf)
      .getOrCreate()

//    println(session.conf.getAll.mkString("\n"))

//    val g = RDFGraphLoader.loadGraphFromDiskAsDataset(session, input)
//    g.triples.toDF().printSchema()
//    g.triples.show(10)
//    val g_inf = new ForwardRuleReasonerRDFSDataset(session).apply(g)
//    println(g_inf.size())

    // load triples from disk
    val graph = RDFGraphLoader.loadFromDisk(input, session, parallelism)


//    println(s"|G| = ${graph.size()}")

    // create reasoner
    val reasoner = profile match {
      case TRANSITIVE => new TransitiveReasoner(session.sparkContext, properties, parallelism)
      case RDFS => new ForwardRuleReasonerRDFS(session.sparkContext, parallelism)
      case RDFS_SIMPLE =>
        val r = new ForwardRuleReasonerRDFS(session.sparkContext, parallelism)
        r.level = RDFSLevel.SIMPLE
        r
      case OWL_HORST => new ForwardRuleReasonerOWLHorst(session.sparkContext)
    }

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)
//    println(s"|G_inf| = ${inferredGraph.size()}")

    // write triples to disk
    RDFGraphWriter.writeGraphToFile(inferredGraph, output.getAbsolutePath, writeToSingleFile, sortedOutput)

    session.stop()
  }

  // the config object
  case class Config(
                     in: Seq[URI] = Seq(),
                     out: File = new File("."),
                     properties: Seq[String] = Seq(),
                     profile: ReasoningProfile = ReasoningProfile.RDFS,
                     writeToSingleFile: Boolean = false,
                     sortedOutput: Boolean = false)

  // read ReasoningProfile enum
  implicit val profilesRead: scopt.Read[ReasoningProfile.Value] =
    scopt.Read.reads(ReasoningProfile forName _.toLowerCase())

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("RDFGraphMaterializer") {

    head("RDFGraphMaterializer", "0.1.0")

    opt[Seq[URI]]('i', "input").required().valueName("<path1>,<path2>,...").
      action((x, c) => c.copy(in = x)).
      text("path to file or directory that contains the input files (in N-Triples format)")

    opt[File]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    opt[Seq[String]]("properties").optional().valueName("<property1>,<property2>,...").
      action((x, c) => {
        c.copy(properties = x)
      }).
      text("list of properties for which the transitive closure will be computed (used only for profile 'transitive')")

    opt[ReasoningProfile]('p', "profile").required().valueName("{rdfs | rdfs-simple | owl-horst | transitive}").
      action((x, c) => c.copy(profile = x)).
      text("the reasoning profile")

    opt[Unit]("single-file").optional().action( (_, c) =>
      c.copy(writeToSingleFile = true)).text("write the output to a single file in the output directory")

    opt[Unit]("sorted").optional().action( (_, c) =>
      c.copy(sortedOutput = true)).text("sorted output of the triples (per file)")

    help("help").text("prints this usage text")

    checkConfig( c =>
      if (c.profile == TRANSITIVE && c.properties.isEmpty) failure("Option --properties must not be empty if profile 'transitive' is set")
      else success )
  }
}
