package net.sansa_stack.inference.spark.abstraction

import scala.collection.mutable

import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD

import net.sansa_stack.inference.data.RDFTriple

/**
  * @author Lorenz Buehmann
  */
trait TypeComputor {

  /**
    * Compute the types for all individuals, taking equivalent individuals into
    * account.
    * @param aboxTriples contains the instance data, i.e. the class assertions and property assertions
    */
  def computeTypes(aboxTriples: RDD[RDFTriple]): RDD[((Set[Node], Set[Node], Set[Node]), Iterable[Node])]

}
