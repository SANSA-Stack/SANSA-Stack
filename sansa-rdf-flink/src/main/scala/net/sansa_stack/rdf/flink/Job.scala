package net.sansa_stack.rdf.flink

import java.io.File

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import net.sansa_stack.rdf.flink.data.{RDFGraphLoader,RDFGraphWriter}

object Job {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(args, config.in, config.out)
      case None =>
        println(parser.usage)
        //run(null, new File("src/test/scala/rdf.nt"), new File("src/test/scala/out"))
    }
  }

  def run(args: Array[String], input: File, output: File): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    //env.getConfig.disableSysoutLogging()

    env.getConfig.setGlobalJobParameters(params)

    // load triples from disk
    val graph = RDFGraphLoader.loadFromFile(input.getAbsolutePath, env)

    val t = graph.getPredicates
    t.print()

    // write triples to disk
    RDFGraphWriter.writeToFile(graph, output.getAbsolutePath)

    env.execute(s"RDF")
  }

  case class Config(
    in: File = new File("."),
    out: File = new File("."))

  val parser = new scopt.OptionParser[Config]("RDF") {
    head("RDF", "0.1.0")

    opt[File]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the input files (in N-Triple format)")

    opt[File]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    help("help").text("prints this usage text")
  }

}
