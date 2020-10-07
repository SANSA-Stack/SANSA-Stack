package net.sansa_stack.owl.spark

import org.apache.spark.sql.Dataset
import org.semanticweb.owlapi.model.OWLAxiom


package object dataset {
  type OWLExpressionsDataset = Dataset[String]
  type OWLAxiomsDataset = Dataset[OWLAxiom]
}
