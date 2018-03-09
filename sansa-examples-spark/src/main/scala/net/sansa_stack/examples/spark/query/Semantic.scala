package net.sansa_stack.examples.spark.query

import java.nio.file.{ FileVisitResult, Files, Path, Paths, SimpleFileVisitor }
import java.nio.file.attribute.BasicFileAttributes
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import org.apache.spark.sql.SparkSession
import java.io.IOException
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.partition.semantic.RdfPartition
import net.sansa_stack.query.spark.semantic.QuerySystem

/*
 * Run SPARQL queries over Spark using Semantic partitioning approach.
 *
 * @author Gezim Sejdiu
 */
object Semantic {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.queries, config.partitions, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, queries: String, partitions: String, output: String): Unit = {

    println("===========================================")
    println("| SANSA - Semantic Partioning example     |")
    println("===========================================")

    // variables initialization
    val numOfFilesPartition: Int = 1
    // val queryInputPath: String = queries //args(1)
    //val partitionedDataPath: String = "src/main/resources/output/partitioned-data/"
    //val queryResultPath: String = "src/main/resources/output/query-result/"
    val symbol = Map(
      "space" -> " " * 5,
      "blank" -> " ",
      "tabs" -> "\t",
      "newline" -> "\n",
      "colon" -> ":",
      "comma" -> ",",
      "hash" -> "#",
      "slash" -> "/",
      "question-mark" -> "?",
      "exclamation-mark" -> "!",
      "curly-bracket-left" -> "{",
      "curly-bracket-right" -> "}",
      "round-bracket-left" -> "(",
      "round-bracket-right" -> ")",
      "less-than" -> "<",
      "greater-than" -> ">",
      "at" -> "@",
      "dot" -> ".",
      "dots" -> "...",
      "asterisk" -> "*",
      "up-arrows" -> "^^")

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("SANSA - Semantic Partioning")
      .getOrCreate()

    // N-Triples reader
    val lang = Lang.NTRIPLES
    val nTriplesRDD = spark.rdf(lang)(input)

    println("----------------------")
    println("Phase 1: RDF Partition")
    println("----------------------")

    // class instance: Class RDFPartition and set the partition data
    val partitionData = new RdfPartition(
      symbol,
      nTriplesRDD,
      partitions,
      numOfFilesPartition).partitionGraph

    // count total number of N-Triples
    countNTriples(Left(nTriplesRDD))
    countNTriples(Right(partitionData))

    println(symbol("newline"))
    println("----------------------")
    println("Phase 2: SPARQL System")
    println("----------------------")

    // class instance: Class QuerySystem
    val qs = new QuerySystem(
      symbol,
      partitionData,
      input,
      output,
      numOfFilesPartition)
    qs.run()

    spark.close()

  }

  // remove path files
  def removePathFiles(root: Path): Unit = {
    if (Files.exists(root)) {
      Files.walkFileTree(root, new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      })
    }
  }

  // count total number of N-Triples
  def countNTriples(dataRDD: Either[RDD[Triple], RDD[String]]): Unit = {
    dataRDD match {
      case Left(x)  => println(s"Number of N-Triples before partition: ${x.distinct.count}")
      case Right(x) => println(s"Number of N-Triples after partition: ${x.distinct.count}")
    }
  }
  case class Config(in: String = "", queries: String = "", partitions: String = "", out: String = "")

  val parser = new scopt.OptionParser[Config]("SANSA - Semantic Partioning example") {

    head(" SANSA - Semantic Partioning example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]('q', "queries").required().valueName("<directory>").
      action((x, c) => c.copy(queries = x)).
      text("the SPARQL query list")

    opt[String]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    opt[String]('p', "partitions").required().valueName("<directory>").
      action((x, c) => c.copy(partitions = x)).
      text("the partitions directory")

    help("help").text("prints this usage text")
  }

}