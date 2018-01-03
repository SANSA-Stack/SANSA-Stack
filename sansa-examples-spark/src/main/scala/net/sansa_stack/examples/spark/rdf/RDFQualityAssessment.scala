package net.sansa_stack.examples.spark.rdf

import org.apache.spark.sql.SparkSession
import java.net.URI
import net.sansa_stack.rdf.spark.io.NTripleReader
import scala.collection.mutable
import java.io.File
import net.sansa_stack.rdf.spark.qualityassessment._

object RDFQualityAssessment {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output: String): Unit = {

    val rdf_quality_file = new File(input).getName

    val spark = SparkSession.builder
      .appName(s"RDF Quality Assessment Example $rdf_quality_file")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("| RDF Quality Assessment Example     |")
    println("======================================")

    val triples = NTripleReader.load(spark, URI.create(input))

    // compute  quality assessment
    val completeness_schema = triples.assessSchemaCompleteness()
    val completeness_interlinking = triples.assessInterlinkingCompleteness()
    val completeness_property = triples.assessPropertyCompleteness()

    val syntacticvalidity_literalnumeric = triples.assessLiteralNumericRangeChecker()

    val syntacticvalidity_XSDDatatypeCompatibleLiterals = triples.assessXSDDatatypeCompatibleLiterals()

    val AssessQualityStr = s"completeness_schema:$completeness_schema \n completeness_interlinking:$completeness_interlinking \n completeness_property:$completeness_property \n"
    println(s"AssessQuality for $rdf_quality_file : $AssessQualityStr")
  }

  case class Config(
    in:  String = "",
    out: String = "")

  val parser = new scopt.OptionParser[Config]("RDF Quality Assessment Example") {

    head("RDF Quality Assessment Example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    help("help").text("prints this usage text")
  }
}