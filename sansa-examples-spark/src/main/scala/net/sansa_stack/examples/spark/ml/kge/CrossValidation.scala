package net.sansa_stack.examples.spark.rdf

import org.apache.spark.sql._
import net.sansa_stack.rdf.spark.kge.triples.Triples
import net.sansa_stack.rdf.spark.kge.convertor.ByIndex
import net.sansa_stack.rdf.spark.kge.crossvalidation.Holdout

object KGE_Holdout {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"Holdout Cross validation techniques example  $input")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("==============================================")
    println("|Holdout Cross validation techniques example |")
    println("==============================================")

    // Assuming the file "test.txt" exists which contains the triples, we load the file
    // the function is provided with the path of the file
    // delimiter = "\t" , indicates file is in tab separated format
    // headers = false , indicates that we ignore the header of the file
    // numeric = false , indicated that content of the file in not in numeric format i.e. (subject,predicate,object) are not numerically given

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
    
    // using the holdout cross validation technique to get 60% of data as training and the rest as testing
    val (train, test) = new Holdout(numericData, 0.6f).crossValidation()

    println("<< DONE >>")

    spark.stop

  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Holdout Cross validation techniques example") {

    head(" Holdout Cross validation techniques example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data")

    help("help").text("prints this usage text")
  }
}