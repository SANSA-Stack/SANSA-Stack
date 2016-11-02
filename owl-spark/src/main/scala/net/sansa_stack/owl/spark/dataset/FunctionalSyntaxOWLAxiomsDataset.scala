package net.sansa_stack.owl.spark.dataset

import net.sansa_stack.owl.common.parsing.FunctionalSyntaxParsing
import org.apache.spark.sql.{Dataset, Encoders}
import org.semanticweb.owlapi.model.OWLAxiom


object FunctionalSyntaxOWLAxiomsDataset extends FunctionalSyntaxParsing {
  def create(expressionsDataset: Dataset[String]): Dataset[OWLAxiom] = {
    implicit val encoder = Encoders.kryo[OWLAxiom]
    expressionsDataset.map(expression => makeAxiom(expression)).
      filter(axiom => axiom != null)
  }
}
