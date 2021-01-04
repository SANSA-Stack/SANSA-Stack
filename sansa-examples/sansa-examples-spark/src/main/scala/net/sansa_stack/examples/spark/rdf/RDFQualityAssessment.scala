package net.sansa_stack.examples.spark.rdf

import java.io.File

import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.qualityassessment._
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession

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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("| RDF Quality Assessment Example     |")
    println("======================================")

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    // compute  quality assessment
    val completeness_schema = triples.assessSchemaCompleteness()
    val completeness_interlinking = triples.assessInterlinkingCompleteness()
    val completeness_property = triples.assessPropertyCompleteness()

    val syntacticvalidity_literalnumeric = triples.assessLiteralNumericRangeChecker()
    val syntacticvalidity_XSDDatatypeCompatibleLiterals = triples.assessXSDDatatypeCompatibleLiterals()

    val availability_DereferenceableUris = triples.assessDereferenceableUris()

    val relevancy_CoverageDetail = triples.assessCoverageDetail()
    val relevancy_CoverageScope = triples.assessCoverageScope()
    val relevancy_AmountOfTriples = triples.assessAmountOfTriples()

    val performance_NoHashURIs = triples.assessNoHashUris()
    val understandability_LabeledResources = triples.assessLabeledResources()

    val AssessQualityStr = s"""
      completeness_schema:$completeness_schema
      completeness_interlinking:$completeness_interlinking
      completeness_property:$completeness_property
      syntacticvalidity_literalnumeric:$syntacticvalidity_literalnumeric
      syntacticvalidity_XSDDatatypeCompatibleLiterals:$syntacticvalidity_XSDDatatypeCompatibleLiterals
      availability_DereferenceableUris:$availability_DereferenceableUris
      relevancy_CoverageDetail:$relevancy_CoverageDetail
      relevancy_CoverageScope:$relevancy_CoverageScope
      relevancy_AmountOfTriples:$relevancy_AmountOfTriples
      performance_NoHashURIs:$performance_NoHashURIs
      understandability_LabeledResources:$understandability_LabeledResources
      """

    println(s"\n AssessQuality for $rdf_quality_file :\n $AssessQualityStr")
  }

  case class Config(
    in: String = "",
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
