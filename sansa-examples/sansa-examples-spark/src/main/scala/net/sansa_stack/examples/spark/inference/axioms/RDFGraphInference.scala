package net.sansa_stack.examples.spark.inference.axioms

import net.sansa_stack.inference.rules.ReasoningProfile
import net.sansa_stack.inference.rules.ReasoningProfile._
import net.sansa_stack.inference.spark.forwardchaining.axioms.{ForwardRuleReasonerOWLHorst, ForwardRuleReasonerRDFS}
import net.sansa_stack.owl.spark.owl._
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object RDFGraphInference {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.profile, config.parallelism)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, profile: ReasoningProfile, parallelism: Int): Unit = {

    // the SPARK config
    val spark = SparkSession.builder
      .appName(s"SPARK $profile Reasoning")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", parallelism)
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.shuffle.partitions", parallelism)
      .getOrCreate()

    // load axioms from disk
    var owlAxioms = spark.owl(Syntax.FUNCTIONAL)(input)
    println(s"|G| = ${owlAxioms.count()}")
    // create reasoner and compute inferred graph
    val inferredGraph = profile match {
      case RDFS => new ForwardRuleReasonerRDFS(spark.sparkContext, parallelism)(owlAxioms)
      case OWL_HORST => new ForwardRuleReasonerOWLHorst(spark.sparkContext, parallelism)(owlAxioms)
      case _ =>
        throw new RuntimeException("Invalid profile: '" + profile + "'")
    }

    println(s"|G_inf| = ${inferredGraph.count()}")

    spark.stop()
  }

  case class Config(
                     in: String = "",
                     profile: ReasoningProfile = ReasoningProfile.RDFS,
                     parallelism: Int = 4)

  // read ReasoningProfile enum
  implicit val profilesRead: scopt.Read[ReasoningProfile.Value] =
    scopt.Read.reads(ReasoningProfile forName _.toLowerCase())

  // the CLI parser
  val parser: OptionParser[Config] = new scopt.OptionParser[Config]("RDFGraphMaterializer") {

    head("RDFGraphMaterializer (axioms)", "0.5.0")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file or directory that contains the input files")

    opt[ReasoningProfile]('p', "profile").required().valueName("{rdfs | owl-horst}").
      action((x, c) => c.copy(profile = x)).
      text("the reasoning profile")

    opt[Int]("parallelism").optional().action((x, c) =>
      c.copy(parallelism = x)).text("the degree of parallelism, i.e. the number of Spark partitions used in the Spark operations")

    help("help").text("prints this usage text")
  }
}
