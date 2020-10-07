package net.sansa_stack.examples.spark.ml.kge

import net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation.{ kFold, Bootstrapping, Holdout }
import net.sansa_stack.rdf.spark.kge.convertor.ByIndex
import net.sansa_stack.rdf.spark.kge.triples.Triples
import org.apache.spark.sql._

object CrossValidation {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.technique, config.k)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, technique: String, k: Int): Unit = {

    val spark = SparkSession.builder
      .appName(s"Cross validation techniques example  $input")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("==============================================")
    println("|Cross validation techniques example |")
    println("==============================================")

    val data = new Triples(input, "\t", false, false, spark)

    // getting 10 distinct entities in (s,p,o) i.e. subjects + objects and printing them
    data.getEntities().take(10).foreach(println)

    // getting 10 distinct predicates in (s,p,o) and printing them
    data.getEntities().take(10).foreach(println)

    // converting the original data to indexData
    val indexedData = new ByIndex(data.triples, spark)
    val numericData = indexedData.numeric()

    // getting 10 distinct (s,p,o) in their numeric (indexed) form and print them
    indexedData.numeric.take(10).foreach(println)

    val (train, test) = technique match {
      case "holdout" => new Holdout(numericData, 0.6f).crossValidation()
      case "bootstrapping" => new Bootstrapping(numericData).crossValidation()
      case "kFold" => new kFold(numericData, k, spark).crossValidation()
      case _ =>
        throw new RuntimeException("'" + technique + "' - Not supported, yet.")
    }

    println("<< DONE >>")

    spark.stop

  }

  case class Config(in: String = "", technique: String = "", k: Int = 0)

  val parser = new scopt.OptionParser[Config]("Cross validation techniques example") {

    head("Cross validation techniques example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data")

    opt[String]('t', "technique").required().valueName("{holdout | bootstrapping | kFold}").
      action((x, c) => c.copy(technique = x)).
      text("cross validation techniques")

    opt[Int]("k").optional().valueName("<value>").
      action((x, c) => {
        c.copy(k = x)
      }).
      text("The k value (used only for technique'kFold')")

    checkConfig(c =>
      if (c.technique == "kFold" && c.k == 0) failure("Option --k-Fold must not be empty if technique 'kFold' is set")
      else success)

    help("help").text("prints this usage text")
  }
}
