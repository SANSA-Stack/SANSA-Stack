package net.sansa_stack.inference.spark.forwardchaining.axioms

import net.sansa_stack.inference.utils.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.model.OWLAxiom

/**
  * A forward chaining implementation for the RDFS entailment regime that works
  * on OWL axioms
  *
  * @param sc The Apache Spark context
  * @param parallelism The degree of parallelism
  */
class ForwardRuleReasonerRDFS(sc: SparkContext, parallelism: Int = 2) extends Logging {

  def apply(sc: SparkContext, parallelism: Int = 2): ForwardRuleReasonerRDFS =
    new ForwardRuleReasonerRDFS(sc, parallelism)

  /**
    * TODO: To be implemented
    */
  def apply(axioms: RDD[OWLAxiom]): RDD[OWLAxiom] = {
    axioms
  }
}
