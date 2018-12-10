package net.sansa_stack.examples.spark.query

import java.io.IOException
import java.nio.file.{ Files, FileVisitResult, Path, Paths, SimpleFileVisitor }
import java.nio.file.attribute.BasicFileAttributes

import net.sansa_stack.query.spark.semantic.QuerySystem
import net.sansa_stack.rdf.common.partition.utils.Symbols
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition._
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Run SPARQL queries over Spark using Semantic partitioning approach.
 *
 * @author Gezim Sejdiu
 */
object Semantic {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.queries, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, queries: String, output: String): Unit = {

    println("===========================================")
    println("| SANSA - Semantic Partioning example     |")
    println("===========================================")

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("SANSA - Semantic Partioning")
      .getOrCreate()

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    println("----------------------")
    println("Phase 1: RDF Partition")
    println("----------------------")

    val partitionData = triples.partitionGraphAsSemantic()

    // count total number of N-Triples
    countNTriples(Left(triples))
    countNTriples(Right(partitionData))

    println("----------------------")
    println("Phase 2: SPARQL System")
    println("----------------------")

    val qs = new QuerySystem(
      symbol = Symbols.symbol,
      partitionData,
      input,
      output,
      numOfFilesPartition = 1)
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
      case Left(x) => println(s"Number of N-Triples before partition: ${x.distinct.count}")
      case Right(x) => println(s"Number of N-Triples after partition: ${x.distinct.count}")
    }
  }
  case class Config(in: String = "", queries: String = "", out: String = "")

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

    help("help").text("prints this usage text")
  }

}
