package net.sansa_stack.owl.spark

import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.model._

package object owlAxioms {

  def extractAxiom (axiom: RDD[OWLAxiom], T: AxiomType[_]): RDD[OWLAxiom] =
    axiom.filter(a => a.getAxiomType.equals(T))

}
