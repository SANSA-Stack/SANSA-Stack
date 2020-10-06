package net.sansa_stack.owl.flink

import org.apache.flink.api.scala.DataSet
import org.semanticweb.owlapi.model.OWLAxiom


package object dataset {
  type OWLExpressionsDataSet = DataSet[String]
  type OWLAxiomsDataSet = DataSet[OWLAxiom]
}
