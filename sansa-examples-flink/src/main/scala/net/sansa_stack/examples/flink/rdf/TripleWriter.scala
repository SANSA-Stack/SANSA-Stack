package net.sansa_stack.examples.flink.rdf
import scala.collection.mutable
import org.apache.flink.api.scala.ExecutionEnvironment
import net.sansa_stack.rdf.flink.data.{ RDFGraphLoader, RDFGraphWriter }

object TripleWriter {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output: String): Unit = {

    println("======================================")
    println("|        Triple writer example       |")
    println("======================================")

    val env = ExecutionEnvironment.getExecutionEnvironment

    val rdfgraph = RDFGraphLoader.loadFromFile(input, env)
    RDFGraphWriter.writeToFile(rdfgraph, output)

    env.execute(s"Triple writer example ($input)")

  }

  case class Config(
    in:  String = "",
    out: String = "")

  val parser = new scopt.OptionParser[Config]("Triple writer example ") {

    head("Triple writer example ")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    help("help").text("prints this usage text")
  }
}
