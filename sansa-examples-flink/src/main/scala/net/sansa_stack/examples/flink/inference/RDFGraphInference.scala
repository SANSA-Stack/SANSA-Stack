package net.sansa_stack.examples.flink.inference

import java.io.{ File, FileInputStream }
import java.net.URI
import java.util.Properties

import scala.io.Source

import com.typesafe.config.ConfigFactory
import net.sansa_stack.inference.flink.data.{ RDFGraphLoader, RDFGraphWriter }
import net.sansa_stack.inference.flink.forwardchaining.{
  ForwardRuleReasonerOWLHorst,
  ForwardRuleReasonerRDFS
}
import net.sansa_stack.inference.rules.{ RDFSLevel, ReasoningProfile }
import net.sansa_stack.inference.rules.ReasoningProfile._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.webmonitor.WebMonitorUtils

object RDFGraphInference {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(
          args,
          config.in,
          config.out,
          config.profile,
          config.writeToSingleFile,
          config.sortedOutput,
          config.propertiesFile,
          config.jobName)
      case None =>
        println(parser.usage)
    }
  }

  def run(
    args: Array[String],
    input: Seq[URI],
    output: URI,
    profile: ReasoningProfile,
    writeToSingleFile: Boolean,
    sortedOutput: Boolean,
    propertiesFile: File,
    jobName: String): Unit = {

    // read reasoner optimization properties
    val reasonerConf =
      if (propertiesFile != null) ConfigFactory.parseFile(propertiesFile)
      else ConfigFactory.load("reasoner")

    // get params
    val params: ParameterTool = ParameterTool.fromArgs(args)

    println("======================================")
    println("|        RDF Graph Inference         |")
    println("======================================")

    val conf = new Configuration()
    conf.setInteger("taskmanager.network.numberOfBuffers", 3000)

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // load triples from disk
    val graph = RDFGraphLoader.loadFromDisk(input, env)
    println(s"|G| = ${graph.size}")

    // create reasoner
    val reasoner = profile match {
      case RDFS | RDFS_SIMPLE =>
        val r = new ForwardRuleReasonerRDFS(env)
        r.useSchemaBroadCasting = reasonerConf.getBoolean("reasoner.rdfs.schema.broadcast")
        r.extractSchemaTriplesInAdvance =
          reasonerConf.getBoolean("reasoner.rdfs.schema.extractTriplesInAdvance")
        if (profile == RDFS_SIMPLE) r.level = RDFSLevel.SIMPLE
        r
      case OWL_HORST => new ForwardRuleReasonerOWLHorst(env)
    }

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)
    println(s"|G_inf| = ${inferredGraph.size}")

    val jn = if (jobName.isEmpty) s"RDF Graph Inference ($profile)" else jobName
  }

  // the config object
  case class Config(
    in: Seq[URI] = Seq(),
    out: URI = new URI("."),
    profile: ReasoningProfile = ReasoningProfile.RDFS,
    writeToSingleFile: Boolean = false,
    sortedOutput: Boolean = false,
    propertiesFile: File = null,
    jobName: String = "") // new File(getClass.getResource("reasoner.properties").toURI)

  // read ReasoningProfile enum
  implicit val profilesRead: scopt.Read[ReasoningProfile.Value] =
    scopt.Read.reads(ReasoningProfile forName _.toLowerCase())

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("RDFGraphMaterializer") {
    head("RDFGraphMaterializer", "0.1.0")

    opt[Seq[URI]]('i', "input")
      .required()
      .valueName("<path>")
      .action((x, c) => c.copy(in = x))
      .text("path to file or directory that contains the input files (in N-Triple format)")

    opt[URI]('o', "out")
      .required()
      .valueName("<directory>")
      .action((x, c) => c.copy(out = x))
      .text("the output directory")

    opt[Unit]("single-file")
      .optional()
      .action((_, c) => c.copy(writeToSingleFile = true))
      .text("write the output to a single file in the output directory")

    opt[Unit]("sorted")
      .optional()
      .action((_, c) => c.copy(sortedOutput = true))
      .text("sorted output of the triples (per file)")

    opt[ReasoningProfile]('p', "profile")
      .required()
      .valueName("{rdfs | rdfs-simple | owl-horst}")
      .action((x, c) => c.copy(profile = x))
      .text("the reasoning profile")

    opt[File]('p', "prop")
      .optional()
      .valueName("<path_to_properties_file>")
      .action((x, c) => c.copy(propertiesFile = x))
      .text("the (optional) properties file which allows some more advanced options")

    opt[String]('j', "jobName")
      .optional()
      .valueName("<name_of_the_Flink_job>")
      .action((x, c) => c.copy(jobName = x))
      .text("the name of the Flink job that occurs also in the Web-UI")

    help("help").text("prints this usage text")

  }
  parser.showUsageOnError
}
