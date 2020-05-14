package net.sansa_stack.ml.spark.similarity.run

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Semantic_Similarity_Estimator {

  // function which is called when object or this file is started
  // is implemented that it only manages a good input parameter handling
  // it uses the parser object to read in parameter and display help advices if needed
  // if parameter config is given, the function run is called which performs main purposes
  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    // set up spark
    val spark = SparkSession.builder
      .appName(s"Semantic Similarity Estimation example  $input") // TODO where is this displayed
      .master("local[*]") // TODO why do we need to specify this?
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate()

    // read in file with function and print certain inforamtion by function
    // code taken from triple reader
    val triples = read_in_nt_triples(input = input, spark = spark, lang = Lang.NTRIPLES)

    spark.stop
    println("Spark session is stopped!")

  }

  def read_in_nt_triples(input: String, spark: SparkSession, lang: Lang): RDD[graph.Triple] = {
    println("Read in file from " + input)

    // specify read in filetype, in this case: and nt file
    val lang = Lang.NTRIPLES
    println("The accepted fileformat seems to be hardcoded to " + lang)

    // read in triples with specified fileformat from input string path
    val triples = spark.rdf(lang)(input)
    println("file has been read in successfully!")

    println("5 Example Lines look like this:")

    triples.take(5).foreach(println(_))

    triples
  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Semantic Similarity Estimation example") {

    head(" Semantic Similarity Estimation example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    help("help").text("prints this usage text")
  }
}
