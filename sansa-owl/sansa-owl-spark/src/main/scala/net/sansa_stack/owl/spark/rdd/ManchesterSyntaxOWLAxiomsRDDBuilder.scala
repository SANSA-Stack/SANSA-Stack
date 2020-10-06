package net.sansa_stack.owl.spark.rdd

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.io.OWLParserException
import org.semanticweb.owlapi.model.{OWLAxiom, OWLRuntimeException}

import net.sansa_stack.owl.common.parsing.ManchesterSyntaxParsing


object ManchesterSyntaxOWLAxiomsRDDBuilder extends ManchesterSyntaxParsing {
  private val logger = Logger(this.getClass)

  def build(spark: SparkSession, filePath: String): OWLAxiomsRDD = {
    val res = ManchesterSyntaxOWLExpressionsRDDBuilder.buildAndGetPrefixes(spark, filePath)

    val expressionsRDD: OWLExpressionsRDD = res._1
    val prefixes: Map[String, String] = res._2

    val defaultPrefix = prefixes.getOrElse(ManchesterSyntaxParsing._empty,
      ManchesterSyntaxParsing.dummyURI)

    expressionsRDD.filter(!_.startsWith("Annotations")).flatMap(frame => {
      try makeAxioms(frame, defaultPrefix)
      catch {
        case exception: OWLParserException =>
          val msg = exception.getMessage
          logger.warn("Parser error for frame\n" + frame + "\n\n" + msg)
          Set.empty[OWLAxiom]
        case exception: OWLRuntimeException =>
          val msg = exception.getMessage
          logger.warn("Parser error for frame\n" + frame + "\n\n" + msg)
          exception.printStackTrace()
          Set.empty[OWLAxiom]
      }
    })
  }
}
