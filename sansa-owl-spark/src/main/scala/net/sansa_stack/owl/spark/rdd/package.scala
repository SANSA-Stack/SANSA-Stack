package net.sansa_stack.owl.spark

import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.model.OWLAxiom


package object rdd {
  type OWLExpressionsRDD = RDD[String]
  type OWLAxiomsRDD = RDD[OWLAxiom]
}
