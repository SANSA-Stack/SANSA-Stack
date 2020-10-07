package net.sansa_stack.inference.spark.abstraction

import org.apache.spark.rdd.RDD

import net.sansa_stack.inference.data.RDFTriple

/**
  * @author Lorenz Buehmann
  */
trait AbstractionGenerator {

  def generateAbstractOntology(triples: RDD[RDFTriple]): RDD[RDFTriple]

}
