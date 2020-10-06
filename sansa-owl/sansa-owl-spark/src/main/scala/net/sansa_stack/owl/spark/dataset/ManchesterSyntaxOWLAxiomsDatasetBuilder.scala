package net.sansa_stack.owl.spark.dataset

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Encoders, SparkSession}
import org.semanticweb.owlapi.io.OWLParserException
import org.semanticweb.owlapi.model.{OWLAxiom, OWLRuntimeException}

import net.sansa_stack.owl.common.parsing.ManchesterSyntaxParsing

object ManchesterSyntaxOWLAxiomsDatasetBuilder extends ManchesterSyntaxParsing {
  private val logger = Logger(this.getClass)

  def build(spark: SparkSession, filePath: String): OWLAxiomsDataset = {
    val res = ManchesterSyntaxOWLExpressionsDatasetBuilder.buildAndGetDefaultPrefix(spark, filePath)
    val expressionsDataset = res._1
    val defaultPrefix = res._2
    build(expressionsDataset, defaultPrefix)
  }

  // FIXME: It has to be ensured that the expressionsDataset is in functional syntax
  def build(expressionsDataset: OWLExpressionsDataset, defaultPrefix: String): OWLAxiomsDataset = {
    implicit val encoder = Encoders.kryo[OWLAxiom]

    expressionsDataset.filter(!_.startsWith("Annotations")).flatMap(frame => {
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
