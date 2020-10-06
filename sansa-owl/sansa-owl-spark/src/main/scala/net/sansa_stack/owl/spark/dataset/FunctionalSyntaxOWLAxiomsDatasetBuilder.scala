package net.sansa_stack.owl.spark.dataset

import org.apache.spark.sql.{Encoders, SparkSession}
import org.semanticweb.owlapi.model.OWLAxiom

import net.sansa_stack.owl.common.parsing.FunctionalSyntaxParsing

object FunctionalSyntaxOWLAxiomsDatasetBuilder extends FunctionalSyntaxParsing {
  def build(spark: SparkSession, filePath: String): OWLAxiomsDataset = {
    build(FunctionalSyntaxOWLExpressionsDatasetBuilder.build(spark, filePath))
  }

  // FIXME: It has to be ensured that the expressionsDataset is in functional syntax
  def build(expressionsDataset: OWLExpressionsDataset): OWLAxiomsDataset = {
    implicit val encoder = Encoders.kryo[OWLAxiom]
    expressionsDataset.map(expression => makeAxiom(expression)).
      filter(axiom => axiom != null)
  }
}
