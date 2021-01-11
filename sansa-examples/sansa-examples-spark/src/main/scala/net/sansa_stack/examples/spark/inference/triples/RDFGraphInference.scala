package net.sansa_stack.examples.spark.inference.triples

import java.net.URI

import net.sansa_stack.inference.rules.ReasoningProfile._
import net.sansa_stack.inference.rules.{RDFSLevel, ReasoningProfile}
import net.sansa_stack.inference.spark.forwardchaining.triples.{ForwardRuleReasonerOWLHorst, ForwardRuleReasonerRDFS, TransitiveReasoner}
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession
import scopt.{OptionParser, Read}

import scala.collection.Seq

object RDFGraphInference {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out, config.profile, config.properties, config.writeToSingleFile, config.sortedOutput, config.parallelism)
      case None =>
        println(parser.usage)
    }
  }

  def run(inputURIs: Seq[URI], output: URI, profile: ReasoningProfile, properties: Seq[Node] = Seq(),
          writeToSingleFile: Boolean, sortedOutput: Boolean, parallelism: Int): Unit = {

    // the SPARK config
    val spark = SparkSession.builder
      .appName(s"SPARK $profile Reasoning")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", parallelism)
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.shuffle.partitions", parallelism)
      .getOrCreate()

    val input = inputURIs.mkString(",")
    // load triples from disk
    val triples = spark.rdf(Lang.NTRIPLES)(input)
    println(s"|G| = ${triples.count()}")

    // create reasoner based on the given profile
    val reasoner = profile match {
      case TRANSITIVE => new TransitiveReasoner(spark.sparkContext, properties, parallelism)
      case RDFS => new ForwardRuleReasonerRDFS(spark.sparkContext, parallelism)
      case RDFS_SIMPLE =>
        val r = new ForwardRuleReasonerRDFS(spark.sparkContext, parallelism)
        r.level = RDFSLevel.SIMPLE
        r
      case OWL_HORST => new ForwardRuleReasonerOWLHorst(spark.sparkContext)
    }

    // compute inferred triples
    val inferredTriples = reasoner.apply(triples)
    println(s"|G_inf| = ${inferredTriples.count()}")

    // write triples to disk in N-Triples format
    inferredTriples.saveAsNTriplesFile(output.toString)

    spark.stop()
  }

  case class Config(
    in: Seq[URI] = Seq(),
    out: URI = new URI("."),
    properties: Seq[Node] = Seq(),
    profile: ReasoningProfile = ReasoningProfile.RDFS,
    writeToSingleFile: Boolean = false,
    sortedOutput: Boolean = false,
    parallelism: Int = 4)

  // read ReasoningProfile enum
  implicit val profilesRead: scopt.Read[ReasoningProfile.Value] =
    scopt.Read.reads(ReasoningProfile forName _.toLowerCase())

  // read node enum
  implicit val nodeRead: Read[Node] = scopt.Read.reads(NodeFactory.createURI(_))

  // the CLI parser
  val parser: OptionParser[Config] = new scopt.OptionParser[Config]("RDFGraphMaterializer") {

    head("RDFGraphMaterializer", "0.1.0")

    opt[Seq[URI]]('i', "input").required().valueName("<path1>,<path2>,...").
      action((x, c) => c.copy(in = x)).
      text("path to file or directory that contains the input files (in N-Triples format)")

    opt[URI]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    opt[Seq[Node]]("properties").optional().valueName("<property1>,<property2>,...").
      action((x, c) => {
        c.copy(properties = x)
      }).
      text("list of properties for which the transitive closure will be computed (used only for profile 'transitive')")

    opt[ReasoningProfile]('p', "profile").required().valueName("{rdfs | rdfs-simple | owl-horst | transitive}").
      action((x, c) => c.copy(profile = x)).
      text("the reasoning profile")

    opt[Unit]("single-file").optional().action((_, c) =>
      c.copy(writeToSingleFile = true)).text("write the output to a single file in the output directory")

    opt[Unit]("sorted").optional().action((_, c) =>
      c.copy(sortedOutput = true)).text("sorted output of the triples (per file)")

    opt[Int]("parallelism").optional().action((x, c) =>
      c.copy(parallelism = x)).text("the degree of parallelism, i.e. the number of Spark partitions used in the Spark operations")

    help("help").text("prints this usage text")

    checkConfig(c =>
      if (c.profile == TRANSITIVE && c.properties.isEmpty) failure("Option --properties must not be empty if profile 'transitive' is set")
      else success)
  }
}
